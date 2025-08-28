import click
from urllib.parse import urlparse
import m3u8
from kinescope import KinescopeVideo, KinescopeDownloader
from pathlib import Path

class URLType(click.ParamType):
    name = 'url'

    def convert(self, value, param, ctx):
        try:
            parsed = urlparse(value)
            if parsed.scheme and parsed.netloc:
                return value
            self.fail(f'Expected valid url. Got {value}', param, ctx)
        except Exception as e:
            self.fail(f'Expected valid url. Got {value}: {e}', param, ctx)


@click.command()
@click.argument('url', type=URLType())
@click.argument('output')

@click.option(
    '--outdir',
    default='./results',
    help='Directory where final videos will be saved'
)
@click.option('--hls-only', is_flag=True, default=False, help='Force HLS mode, skip DASH')
@click.option('--dash-only', is_flag=True, default=False, help='Force DASH mode, skip HLS')
@click.option('--audio-lang', default=None, help='Preferred audio language code, e.g. ru or en')
@click.option('--force', is_flag=True, default=False, help='Overwrite output if exists')

@click.option(
    '--referer', '-r',
    required=False,
    help='Referer url of the site where the video is embedded',
    type=URLType()
)
@click.option(
    '--best-quality',
    default=False,
    is_flag=True,
    help='Automatically select the best possible quality'
)
@click.option(
    '--temp',
    default='./temp',
    help='Temporary directory for intermediate files'
)

def main(url, output, referer, best_quality, temp, hls_only, dash_only, audio_lang, force, outdir):
    outdir_path = Path(outdir)
    outdir_path.mkdir(parents=True, exist_ok=True)

    out_path = outdir_path / Path(output).with_suffix('.mp4')
    if out_path.exists() and not force:
        raise click.UsageError(f'Output exists: {out_path}')

    kv = KinescopeVideo(url=url, referer_url=referer)
    downloader = KinescopeDownloader(kv, temp_dir=temp)
    downloader.preferred_audio_lang = audio_lang

    if hls_only:
        downloader.playlist_url = downloader.kinescope_video.get_hls_master_playlist_url()
        downloader.playlist_type = 'hls'
        downloader.mpd_master = None
    elif dash_only:
        downloader.playlist_url = downloader.kinescope_video.get_mpd_master_playlist_url()
        downloader.playlist_type = 'dash'
        downloader.mpd_master = downloader._fetch_mpd_master()

    if getattr(downloader, 'playlist_type', None) == 'hls':
        print('= OPTIONS ============================')
        variants = downloader.get_hls_variants()  # [(res,(w,h)|None), bw, video_uri, audio_uri]
        if variants:
            # Подсчёт длительности и примерного размера для каждого variant-плейлиста
            computed = []
            for res, bw, v_uri, a_uri in variants:
                try:
                    r = downloader.http.get(v_uri, headers={"Referer": downloader._cdn_referer}, timeout = downloader._req_timeout)
                    r.raise_for_status()
                    sub_pl = m3u8.loads(r.text)
                    total_dur = sum(s.duration for s in sub_pl.segments)
                    size_mb = round((bw / 8 * total_dur) / (1024 * 1024), 1)
                except Exception:
                    total_dur = None
                    size_mb = None
                computed.append((res, bw, v_uri, a_uri, total_dur, size_mb))

            for i, (res, bw, v_uri, a_uri, total_dur, size_mb) in enumerate(computed):
                label = f"{res[0]}x{res[1]}" if res else "unknown"
                if size_mb is not None:
                    print(f'[{i}] {label} ({bw//1000} kbps, ~{size_mb} MB)')
                else:
                    print(f'[{i}] {label} ({bw//1000} kbps)')

            if best_quality:
                res, bw, v_uri, a_uri, total_dur, size_mb = computed[-1]
                label = f"{res[0]}x{res[1]}" if res else "unknown"
                if size_mb is not None:
                    print(f"[*] Auto-selected: {label} ({bw//1000} kbps, ~{size_mb} MB)")
                else:
                    print(f"[*] Auto-selected: {label} ({bw//1000} kbps)")
                chosen_res = res
            else:
                idx = int(input('Choose HLS variant index: '))
                res, bw, v_uri, a_uri, total_dur, size_mb = computed[idx]
                chosen_res = res

            print('======================================')
            downloader.download(str(out_path), resolution=chosen_res if chosen_res else None)
        else:
            print('[*] HLS master without variants; using master as-is')
            print('======================================')
            downloader.download(str(out_path))
        return

    video_resolutions = downloader.get_resolutions()
    if not video_resolutions:
        raise RuntimeError('No resolutions available (DASH)')

    print('= OPTIONS ============================')
    for i, (w, h) in enumerate(video_resolutions):
        print(f'[{i}] {w}x{h}')
    if best_quality:
        chosen_resolution = video_resolutions[-1]
    else:
        idx = int(input('Choose resolution index: '))
        chosen_resolution = video_resolutions[idx]
    print(f'[*] {chosen_resolution[1]}p is selected')
    print('======================================')

    downloader.download(str(out_path), resolution=chosen_res)


if __name__ == '__main__':
    main()