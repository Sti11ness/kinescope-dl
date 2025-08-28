import sys
import m3u8
from io import BytesIO
from os import PathLike
from typing import Union, Tuple, Optional, List
from pathlib import Path
from requests import Session
import subprocess
from subprocess import Popen
from shutil import copyfileobj, rmtree
from base64 import b64decode, b64encode
from requests.exceptions import ChunkedEncodingError
from urllib.parse import urljoin
from tqdm import tqdm

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import shutil

from mpegdash.parser import MPEGDASHParser, MPEGDASH

from kinescope.kinescope import KinescopeVideo
from kinescope.const import KINESCOPE_BASE_URL
from kinescope.exceptions import (
    FFmpegNotFoundError,
    Mp4DecryptNotFoundError,
    SegmentDownloadError,
    InvalidResolution,
    VideoNotFound,
)

class VideoDownloader:
    def __init__(self,
                 kinescope_video: KinescopeVideo,
                 temp_dir: Union[str, PathLike] = './temp',
                 ffmpeg_path: Union[str, PathLike] = './ffmpeg',
                 mp4decrypt_path: Union[str, PathLike] = './mp4decrypt'):
        self.kinescope_video: KinescopeVideo = kinescope_video

        self.temp_path: Path = Path(temp_dir)
        self.temp_path.mkdir(parents=True, exist_ok=True)

        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            meipass_path = Path(sys._MEIPASS).resolve()
            self.ffmpeg_path = meipass_path / 'ffmpeg'
            self.mp4decrypt_path = meipass_path / 'mp4decrypt'
        else:
            self.ffmpeg_path = Path(ffmpeg_path)
            self.mp4decrypt_path = Path(mp4decrypt_path)

        self.http = self._init_http()
        self._req_timeout = (5, 20)

        self.http.headers.update({'Accept-Encoding': 'identity'})

        self._cdn_referer = f"{KINESCOPE_BASE_URL}/{self.kinescope_video.video_id}?autoplay=1"

        self.playlist_url, self.playlist_type = self._detect_master()
        self.base_url = self.playlist_url.rsplit('/', 1)[0] + '/'

        self.mpd_master: Optional[MPEGDASH] = None
        if self.playlist_type == 'dash':
            self.mpd_master = self._fetch_mpd_master()

        self.preferred_audio_lang: Optional[str] = None
        self.mode_force: Optional[str] = None  # 'hls'|'dash'|None

    def _ffprobe_path(self) -> Optional[str]:
        # 1) nearby ffmpeg
        cand = [
            self.ffmpeg_path.parent / "ffprobe",
            self.ffmpeg_path.parent / "ffprobe.exe",
        ]
        # 2) from PATH
        for name in ("ffprobe", "ffprobe.exe"):
            p = shutil.which(name)
            if p:
                cand.append(Path(p))

        for c in cand:
            try:
                if c and Path(c).exists():
                    return str(c)
            except Exception:
                pass
        return None

    def _init_http(self) -> Session:
        s = Session()
        s.headers.update({
            'Accept-Encoding': 'identity',
            'User-Agent': 'kinescope-dl/0.3 (+https://github.com/your/fork)'
        })
        retry = Retry(
            total=5, connect=5, read=5, backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(['GET', 'POST'])
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        return s

    def __del__(self):
        try:
            rmtree(self.temp_path)
        except Exception:
            pass

    def _merge_tracks(self,
                      source_video_filepath: Union[str, PathLike],
                      source_audio_filepath: Union[str, PathLike],
                      target_filepath: Union[str, PathLike]):
        try:
            Popen((
                str(self.ffmpeg_path),
                "-i", str(source_video_filepath),
                "-i", str(source_audio_filepath),
                "-c", "copy",
                str(target_filepath),
                "-y", "-loglevel", "error"
            )).communicate()
        except FileNotFoundError:
            raise FFmpegNotFoundError('FFmpeg binary was not found at the specified path')

    def _calc_hls_duration(self, m3u8_url: str) -> float:
        r = self.http.get(m3u8_url, headers={"Referer": self._cdn_referer}, timeout=self._req_timeout)
        r.raise_for_status()
        pl = m3u8.loads(r.text)
        return float(sum(s.duration for s in pl.segments))

    def _download_hls_via_ffmpeg(self,
                                video_m3u8_url: str,
                                audio_m3u8_url: Optional[str],
                                target_filepath: Union[str, PathLike]):
        def _dur(url: str) -> float:
            r = self.http.get(url, headers={"Referer": self._cdn_referer}, timeout=self._req_timeout)
            r.raise_for_status()
            pl = m3u8.loads(r.text)
            return float(sum(s.duration for s in pl.segments))

        video_dur = 0.0
        audio_dur = 0.0
        try:
            video_dur = _dur(video_m3u8_url)
        except Exception:
            pass
        if audio_m3u8_url:
            try:
                audio_dur = _dur(audio_m3u8_url)
            except Exception:
                pass
        total_duration = max(video_dur, audio_dur, 0.0)

        args: list[str] = [
            str(self.ffmpeg_path),
            "-headers", f"Referer: {self._cdn_referer}",
            "-i", video_m3u8_url
        ]
        if audio_m3u8_url:
            args += [
                "-headers", f"Referer: {self._cdn_referer}",
                "-i", audio_m3u8_url,
                "-map", "0:v:0",
                "-map", "1:a:0?",
                "-map", "0:a:0?",
            ]
        else:
            args += [
                "-map", "0:v:0",
                "-map", "0:a:0?",
            ]
        args += [
            "-c", "copy",
            "-sn",
            "-f", "mp4",            # формат явно mp4, чтобы .part не ломало маппер контейнера
            str(target_filepath),
            "-y",
            "-progress", "pipe:1",
            "-nostats",
            "-loglevel", "error"
        ]
        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )

        try:
            if total_duration > 0:
                with tqdm(total=int(total_duration), unit="sec", desc="HLS") as bar:
                    if proc.stdout is not None:
                        for line in proc.stdout:
                            if line.startswith("out_time_ms"):
                                ms = int(line.split("=")[1].strip())
                                sec = int(ms / 1_000_000)
                                if sec <= total_duration:
                                    bar.n = sec
                                    bar.refresh()
            else:
                if proc.stdout is not None:
                    last_sec = -1
                    for line in proc.stdout:
                        if line.startswith("out_time_ms"):
                            ms = int(line.split("=")[1].strip())
                            sec = int(ms / 1_000_000)
                            if sec != last_sec:
                                last_sec = sec
                                sys.stdout.write(f"\r[*] Downloaded: {sec} sec")
                                sys.stdout.flush()
                    print()
        finally:
            return_code = proc.wait()

        if return_code != 0:
            err = ""
            try:
                if proc.stderr:
                    err = "".join(proc.stderr.readlines()[-20:])
            except Exception:
                pass
            try:
                Path(target_filepath).unlink(missing_ok=True)
            except Exception:
                pass
            raise RuntimeError(f"ffmpeg failed with code {return_code}. Trace:\n{err}")

    def _decrypt_video(self,
                       source_filepath: Union[str, PathLike],
                       target_filepath: Union[str, PathLike],
                       key: str):
        try:
            Popen((
                str(self.mp4decrypt_path),
                "--key", f"1:{key}",
                str(source_filepath),
                str(target_filepath)
            )).communicate()
        except FileNotFoundError:
            raise Mp4DecryptNotFoundError('mp4decrypt binary was not found at the specified path')

    def _get_license_key(self) -> Optional[str]:
        if not self.mpd_master:
            return None
        cps = self.mpd_master.periods[0].adaptation_sets[0].content_protections
        if not cps:
            return None
        kid_hex = self.mpd_master.periods[0].adaptation_sets[0].content_protections[0].cenc_default_kid.replace('-', '')
        payload = {
            'kids': [b64encode(bytes.fromhex(kid_hex)).decode().replace('=', '')],
            'type': 'temporary'
        }
        r = self.http.post(
            url=self.kinescope_video.get_clearkey_license_url(),
            headers={'origin': KINESCOPE_BASE_URL},
            json=payload,
            timeout=self._req_timeout
        )
        return b64decode(r.json()['keys'][0]['k'] + '==').hex()

    def _fetch_segment(self, segment_url: str, file):
        for _ in range(5):
            try:
                url = segment_url if segment_url.startswith("http") else urljoin(self.base_url, segment_url)
                resp = self.http.get(
                    url,
                    stream=True,
                    headers={'Referer': self._cdn_referer},
                    timeout=self._req_timeout
                )
                copyfileobj(BytesIO(resp.content), file)
                return
            except ChunkedEncodingError:
                pass
        raise SegmentDownloadError(f'Failed to download segment {segment_url}')

    def _fetch_segments(self,
                        segments_urls: list[str],
                        filepath: Union[str, PathLike],
                        progress_bar_label: str = ''):
        segments_urls = [seg for i, seg in enumerate(segments_urls) if seg and i == segments_urls.index(seg)]
        with open(filepath, 'wb') as f:
            with tqdm(
                desc=progress_bar_label,
                total=len(segments_urls),
                bar_format='{desc}: {percentage:3.0f}%|{bar:10}| [{n_fmt}/{total_fmt}]'
            ) as progress_bar:
                for segment_url in segments_urls:
                    self._fetch_segment(segment_url, f)
                    progress_bar.update()

    def _get_segments_urls(self, resolution: Tuple[int, int]) -> dict[str, list[str]]:
        try:
            return {
                adaptation_set.mime_type: [
                    seg.media for seg in adaptation_set.representations[
                        [(r.width, r.height) for r in adaptation_set.representations].index(resolution)
                        if adaptation_set.representations[0].height else 0
                    ].segment_lists[0].segment_urls if seg.media is not None
                ] for adaptation_set in self.mpd_master.periods[0].adaptation_sets
            }
        except ValueError:
            raise InvalidResolution('Invalid resolution specified')

    def _fetch_mpd_master(self) -> MPEGDASH:
        txt = self.http.get(
            url=self.playlist_url, 
            timeout=self._req_timeout,
            headers={'Referer': self._cdn_referer, 'Accept-Encoding': 'identity'}
        ).text
        return MPEGDASHParser.parse(txt)

    def _detect_master(self) -> Tuple[str, str]:
        hls_url = self.kinescope_video.get_hls_master_playlist_url()
        r = self.http.get(hls_url, 
                          headers={'Referer': self._cdn_referer, 'Accept-Encoding': 'identity'}, 
                          timeout=self._req_timeout)
        if r.status_code == 200 and ('#EXTM3U' in r.text or 'application/vnd.apple.mpegurl' in r.headers.get('Content-Type', '')):
            return hls_url, 'hls'
        dash_url = self.kinescope_video.get_mpd_master_playlist_url()
        r = self.http.get(dash_url, 
                          headers={'Referer': self._cdn_referer, 'Accept-Encoding': 'identity'}, 
                          timeout=self._req_timeout)
        if r.status_code == 200 and ('<MPD' in r.text):
            return dash_url, 'dash'
        raise VideoNotFound('Master playlist not found (neither HLS nor DASH)')

    def _load_hls_master(self) -> m3u8.M3U8:
        r = self.http.get(self.playlist_url, 
                          headers={"Referer": self._cdn_referer}, 
                          timeout=self._req_timeout)
        r.raise_for_status()
        return m3u8.loads(r.text)

    def get_hls_variants(self) -> list[tuple[Optional[Tuple[int, int]], int, str, Optional[str]]]:
        pl = self._load_hls_master()
        variants: list[tuple[Optional[Tuple[int, int]], int, str, Optional[str]]] = []

        audio_by_group: dict[str, list] = {}
        for m in pl.media:
            if m.type == 'AUDIO' and getattr(m, 'group_id', None):
                audio_by_group.setdefault(m.group_id, []).append(m)

        def pick_audio_uri_for_group(group_id: Optional[str]) -> Optional[str]:
            def _lang_rank(x):
                lang = getattr(x, 'language', None)
                exact = self.preferred_audio_lang and lang and lang.lower() == self.preferred_audio_lang.lower()
                prefix = self.preferred_audio_lang and lang and lang.lower().startswith(self.preferred_audio_lang.lower())
                return (
                    0 if exact else (1 if prefix else 2),
                    0 if getattr(x, 'default', False) else 1,
                    0 if getattr(x, 'autoselect', False) else 1
                )

            if group_id and group_id in audio_by_group:
                medias = [m for m in audio_by_group[group_id] if getattr(m, 'uri', None)]
                if not medias:
                    return None
                cand = sorted(medias, key=_lang_rank)[0]
                return urljoin(self.playlist_url, cand.uri)

            if len(audio_by_group) == 1:
                only_group = next(iter(audio_by_group.values()))
                medias = [m for m in only_group if getattr(m, 'uri', None)]
                if medias:
                    cand = sorted(medias, key=_lang_rank)[0]
                    return urljoin(self.playlist_url, cand.uri)
            return None

        for v in pl.playlists:
            res = None
            if v.stream_info.resolution:
                res = (int(v.stream_info.resolution[0]), int(v.stream_info.resolution[1]))
            bw = int(v.stream_info.bandwidth or 0)
            video_uri = urljoin(self.playlist_url, v.uri)
            group_id = getattr(v.stream_info, "audio", None)
            audio_uri = pick_audio_uri_for_group(group_id)
            variants.append((res, bw, video_uri, audio_uri))

        variants.sort(key=lambda x: (x[0][1] if x[0] else 0, x[1]))
        return variants

    def _select_hls_variant_urls(self, desired_resolution: Optional[Tuple[int, int]]) -> tuple[str, Optional[str]]:
        variants = self.get_hls_variants()
        if not variants:
            return self.playlist_url, None
        if desired_resolution:
            target_h = desired_resolution[1]
            leq = [v for v in variants if v[0] and v[0][1] <= target_h]
            chosen = leq[-1] if leq else variants[-1]
        else:
            chosen = variants[-1]
        return chosen[2], chosen[3]

    def get_resolutions(self) -> Optional[list[Tuple[int, int]]]:
        if self.playlist_type == 'dash' and self.mpd_master:
            for adaptation_set in self.mpd_master.periods[0].adaptation_sets:
                if adaptation_set.representations[0].height:
                    return [(r.width, r.height) for r in sorted(adaptation_set.representations, key=lambda r: r.height)]
        return None

    def _verify_output_or_raise(self, filepath: Path, require_audio: bool = True):
        probe = self._ffprobe_path()
        if not probe:
            print("[*] Warning: ffprobe not found. Skipping validation.")
            return

        try:
            proc = subprocess.run(
                [probe, "-v", "error", "-show_streams", "-show_format", "-of", "json", str(filepath)],
                capture_output=True, text=True, check=True
            )
        except Exception as e:
            # if ffprobe didnt start — dont fall it just skip validation
            print(f"[*] Warning: ffprobe failed: {e}. Skipping validation.")
            return

        import json
        meta = json.loads(proc.stdout or '{}')
        streams = meta.get('streams', [])
        has_video = any(s.get('codec_type') == 'video' for s in streams)
        has_audio = any(s.get('codec_type') == 'audio' for s in streams)
        try:
            duration = float(meta.get('format', {}).get('duration', '0') or 0.0)
        except Exception:
            duration = 0.0

        if not has_video or duration <= 0.5 or (require_audio and not has_audio):
            try:
                filepath.unlink(missing_ok=True)
            except Exception:
                pass
            raise RuntimeError('Output validation failed (no video/audio or zero duration)')
        
    def download(self, filepath: str, resolution: Optional[Tuple[int, int]] = None):
        filepath = Path(filepath).with_suffix('.mp4')
        filepath.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = filepath.with_suffix(filepath.suffix + '.part')
        vid = self.kinescope_video.video_id

        if self.playlist_type == 'hls':
            try:
                video_url, audio_url = self._select_hls_variant_urls(resolution)
                self._download_hls_via_ffmpeg(video_url, audio_url, tmp_path)
                print('[*] Saved via HLS')
                self._verify_output_or_raise(tmp_path)
                tmp_path.rename(filepath)
                return
            except Exception as e:
                print(f'[*] HLS failed: {e}\n[*] Falling back to DASH...')
                self.playlist_url = self.kinescope_video.get_mpd_master_playlist_url()
                self.playlist_type = 'dash'
                self.mpd_master = self._fetch_mpd_master()

        try:
            if not resolution:
                res_list = self.get_resolutions()
                if not res_list:
                    raise InvalidResolution('Resolutions are not available for DASH manifest')
                resolution = res_list[-1]

            key = self._get_license_key()

            video_target = self.temp_path / f'{vid}_video.mp4{".enc" if key else ""}'
            audio_target = self.temp_path / f'{vid}_audio.mp4{".enc" if key else ""}'

            segments = self._get_segments_urls(resolution)
            video_segments = segments.get('video/mp4', [])
            audio_segments = segments.get('audio/mp4', [])

            self._fetch_segments(video_segments, video_target, 'Video')

            if audio_segments:
                self._fetch_segments(audio_segments, audio_target, 'Audio')

            if key:
                print('[*] Decrypting...', end=' ')
                self._decrypt_video(
                    self.temp_path / f'{vid}_video.mp4.enc',
                    self.temp_path / f'{vid}_video.mp4',
                    key
                )
                if audio_segments:
                    self._decrypt_video(
                        self.temp_path / f'{vid}_audio.mp4.enc',
                        self.temp_path / f'{vid}_audio.mp4',
                        key
                    )
                print('Done')

            if audio_segments:
                print('[*] Merging tracks...', end=' ')
                self._merge_tracks(
                    self.temp_path / f'{vid}_video.mp4',
                    self.temp_path / f'{vid}_audio.mp4',
                    tmp_path
                )
                print('Done')
            else:
                Path(self.temp_path / f'{vid}_video.mp4').rename(tmp_path)

            self._verify_output_or_raise(tmp_path)
            tmp_path.rename(filepath)
            print('[*] Saved via DASH')
            return

        except Exception as e:
            print(f'[*] DASH failed: {e}\n[*] Trying HLS...')
            self.playlist_url = self.kinescope_video.get_hls_master_playlist_url()
            self.playlist_type = 'hls'
            video_url, audio_url = self._select_hls_variant_urls(resolution)
            self._download_hls_via_ffmpeg(video_url, audio_url, tmp_path)
            self._verify_output_or_raise(tmp_path)
            tmp_path.rename(filepath)
            print('[*] Saved via HLS (fallback)')
            return
