import os
import time
import math
import threading
from pathlib import Path
from typing import Mapping, Optional, List, Tuple

import requests


class ParallelResumableDownloader:
    def __init__(
        self,
        num_workers: int = 4,
        chunk_size: int = 1024 * 1024,
        connect_timeout: int = 10,
        read_timeout: int = 30,
        retry_wait_seconds: int = 5,
        max_retries_per_worker: Optional[int] = None,
        user_agent: str = "ParallelResumableDownloader/1.0",
    ) -> None:
        self.num_workers = num_workers
        self.chunk_size = chunk_size
        self.timeout = (connect_timeout, read_timeout)
        self.retry_wait_seconds = retry_wait_seconds
        self.max_retries_per_worker = max_retries_per_worker
        self.headers = {"User-Agent": user_agent}

        self._progress_lock = threading.Lock()
        self._print_lock = threading.Lock()
        self._progress = {}

    def download(self, url: str, output_path: str) -> None:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        file_size, accepts_ranges = self._get_remote_file_info(url)

        if file_size == 0:
            output.write_bytes(b"")
            with self._print_lock:
                print(f"\n✅ Descarga completada: {output}")
            return

        if not accepts_ranges:
            with self._print_lock:
                print(
                    "El servidor no soporta descargas parciales con Range. "
                    "Se usara descarga secuencial."
                )
            self._download_single(url, output, file_size)
            return

        temp_dir = output.parent / f"{output.name}.parts"
        temp_dir.mkdir(parents=True, exist_ok=True)

        ranges = self._split_ranges(file_size, self.num_workers)
        part_files = [temp_dir / f"part_{i}.bin" for i in range(len(ranges))]

        for i, (start, end) in enumerate(ranges):
            self._progress[i] = self._current_segment_size(part_files[i], start, end)

        stop_event = threading.Event()
        errors = []
        threads = []

        for i, (start, end) in enumerate(ranges):
            t = threading.Thread(
                target=self._download_segment_with_retries,
                args=(url, i, start, end, part_files[i], stop_event, errors),
                daemon=True,
            )
            t.start()
            threads.append(t)

        progress_thread = threading.Thread(
            target=self._progress_monitor,
            args=(file_size, ranges, part_files, stop_event),
            daemon=True,
        )
        progress_thread.start()

        for t in threads:
            t.join()

        stop_event.set()
        progress_thread.join(timeout=1)

        if errors:
            raise RuntimeError(
                f"Fallo en la descarga. Los fragmentos parciales se conservan en: {temp_dir}\n"
                f"Primer error: {errors[0]}"
            )

        self._merge_parts(output, part_files)

        with self._print_lock:
            print(f"\n✅ Descarga completada: {output}")

    def _download_single(self, url: str, output: Path, file_size: int) -> None:
        temp_output = output.with_suffix(output.suffix + ".partial")
        retries = 0

        while True:
            downloaded = 0

            try:
                if temp_output.exists():
                    temp_output.unlink()

                with requests.get(
                    url,
                    headers=self.headers,
                    stream=True,
                    timeout=self.timeout,
                ) as response:
                    if response.status_code >= 400:
                        raise RuntimeError(
                            f"El servidor rechazo la descarga secuencial. HTTP {response.status_code}"
                        )

                    with open(temp_output, "wb") as f:
                        for chunk in response.iter_content(chunk_size=self.chunk_size):
                            if not chunk:
                                continue

                            f.write(chunk)
                            f.flush()
                            os.fsync(f.fileno())

                            downloaded += len(chunk)
                            percent = downloaded * 100 / file_size if file_size else 0

                            with self._print_lock:
                                print(
                                    f"\rDescargado total: {downloaded}/{file_size} bytes "
                                    f"({percent:5.1f}%)",
                                    end="",
                                    flush=True,
                                )

                temp_output.replace(output)

                with self._print_lock:
                    print(f"\n✅ Descarga completada: {output}")

                return

            except KeyboardInterrupt:
                raise

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                OSError,
                RuntimeError,
            ) as e:
                retries += 1

                if self.max_retries_per_worker is not None and retries > self.max_retries_per_worker:
                    raise RuntimeError(f"Fallo en la descarga secuencial: {e}") from e

                wait = min(self.retry_wait_seconds * (2 ** (retries - 1)), 300)

                with self._print_lock:
                    print(
                        f"\n⚠️ Descarga secuencial falló: {e} | "
                        f"reintento {retries} en {wait}s"
                    )

                time.sleep(wait)

    def _get_remote_file_info(self, url: str) -> Tuple[int, bool]:
        with requests.Session() as session:
            session.headers.update(self.headers)

            response_headers: Mapping[str, str] = {}
            response_status = None

            try:
                with session.head(url, allow_redirects=True, timeout=self.timeout) as response:
                    response_status = response.status_code
                    response_headers = response.headers
            except requests.RequestException:
                pass

            file_size = self._extract_remote_file_size(response_headers)
            accept_ranges = response_headers.get("Accept-Ranges", "").lower()
            supports_range = (
                "bytes" in accept_ranges
                or response_status == 206
                or response_headers.get("Content-Range") is not None
            )

            with session.get(
                url,
                headers={"Range": "bytes=0-0", **self.headers},
                stream=True,
                timeout=self.timeout,
            ) as probe_response:
                if probe_response.status_code >= 400 and file_size is None:
                    raise RuntimeError(
                        f"No se pudo consultar el archivo remoto. HTTP {probe_response.status_code}"
                    )

                if file_size is None:
                    file_size = self._extract_remote_file_size(probe_response.headers)

                if probe_response.status_code == 206:
                    supports_range = True

            if file_size is None:
                raise RuntimeError("No se pudo determinar el tamaño del archivo remoto.")

            return file_size, supports_range

    @staticmethod
    def _extract_remote_file_size(headers: Mapping[str, str]) -> Optional[int]:
        content_length = headers.get("Content-Length")
        content_range = headers.get("Content-Range")

        if content_range and "/" in content_range:
            try:
                return int(content_range.split("/")[-1])
            except ValueError:
                pass

        if content_length:
            try:
                return int(content_length)
            except ValueError:
                pass

        return None

    @staticmethod
    def _split_ranges(file_size: int, workers: int) -> List[Tuple[int, int]]:
        part_size = math.ceil(file_size / workers)
        ranges = []

        for i in range(workers):
            start = i * part_size
            end = min(start + part_size - 1, file_size - 1)
            if start <= end:
                ranges.append((start, end))

        return ranges

    @staticmethod
    def _current_segment_size(part_file: Path, start: int, end: int) -> int:
        expected = end - start + 1
        if not part_file.exists():
            return 0
        return min(part_file.stat().st_size, expected)

    def _download_segment_with_retries(
        self,
        url: str,
        index: int,
        start: int,
        end: int,
        part_file: Path,
        stop_event: threading.Event,
        errors: list,
    ) -> None:
        retries = 0
        expected_size = end - start + 1

        while not stop_event.is_set():
            try:
                current_size = self._current_segment_size(part_file, start, end)

                if current_size >= expected_size:
                    with self._progress_lock:
                        self._progress[index] = expected_size
                    return

                range_start = start + current_size
                headers = dict(self.headers)
                headers["Range"] = f"bytes={range_start}-{end}"

                with requests.get(url, headers=headers, stream=True, timeout=self.timeout) as response:
                    if response.status_code != 206:
                        raise RuntimeError(
                            f"El servidor no devolvió 206 Partial Content para el segmento {index}. "
                            f"HTTP {response.status_code}"
                        )

                    with open(part_file, "ab") as f:
                        for chunk in response.iter_content(chunk_size=self.chunk_size):
                            if stop_event.is_set():
                                return
                            if not chunk:
                                continue
                            f.write(chunk)
                            f.flush()
                            os.fsync(f.fileno())

                            current_size += len(chunk)
                            with self._progress_lock:
                                self._progress[index] = min(current_size, expected_size)

                final_size = self._current_segment_size(part_file, start, end)
                if final_size >= expected_size:
                    with self._progress_lock:
                        self._progress[index] = expected_size
                    return

            except KeyboardInterrupt:
                stop_event.set()
                errors.append("Interrumpido por usuario")
                return

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                OSError,
                RuntimeError,
            ) as e:
                retries += 1

                if self.max_retries_per_worker is not None and retries > self.max_retries_per_worker:
                    stop_event.set()
                    errors.append(f"Segmento {index}: {e}")
                    return

                wait = min(self.retry_wait_seconds * (2 ** (retries - 1)), 300)

                with self._print_lock:
                    print(
                        f"\n⚠️ Segmento {index} falló: {e} | "
                        f"reintento {retries} en {wait}s"
                    )

                time.sleep(wait)

    def _progress_monitor(
        self,
        total_size: int,
        ranges: List[Tuple[int, int]],
        part_files: List[Path],
        stop_event: threading.Event,
    ) -> None:
        expected_sizes = [(end - start + 1) for start, end in ranges]

        while not stop_event.is_set():
            downloaded = 0
            parts_status = []

            with self._progress_lock:
                for i, expected in enumerate(expected_sizes):
                    current = self._current_segment_size(part_files[i], ranges[i][0], ranges[i][1])
                    self._progress[i] = min(current, expected)
                    downloaded += self._progress[i]
                    parts_status.append(f"{i}:{self._progress[i]}/{expected}")

            percent = downloaded * 100 / total_size if total_size else 0

            with self._print_lock:
                print(
                    f"\rDescargado total: {downloaded}/{total_size} bytes "
                    f"({percent:5.1f}%) | " + " | ".join(parts_status),
                    end="",
                    flush=True,
                )

            if downloaded >= total_size:
                return

            time.sleep(1)

    @staticmethod
    def _merge_parts(output: Path, part_files: List[Path]) -> None:
        temp_output = output.with_suffix(output.suffix + ".assembled")

        with open(temp_output, "wb") as outfile:
            for part_file in part_files:
                with open(part_file, "rb") as infile:
                    while True:
                        chunk = infile.read(1024 * 1024)
                        if not chunk:
                            break
                        outfile.write(chunk)

        temp_output.replace(output)

    @staticmethod
    def cleanup_parts(output_path: str) -> None:
        output = Path(output_path)
        temp_dir = output.parent / f"{output.name}.parts"
        if temp_dir.exists():
            for f in temp_dir.iterdir():
                if f.is_file():
                    f.unlink()
            temp_dir.rmdir()


if __name__ == "__main__":
    url = "https://ejemplo.com/archivo_grande.zip"
    destino = "archivo_grande.zip"

    downloader = ParallelResumableDownloader(
        num_workers=4,
        chunk_size=1024 * 1024,
        connect_timeout=10,
        read_timeout=30,
        retry_wait_seconds=5,
        max_retries_per_worker=None,  # None = infinitos
    )

    downloader.download(url, destino)
