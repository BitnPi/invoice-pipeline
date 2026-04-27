"""File watcher for monitoring invoice folders."""

import time
import logging
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional
from queue import Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

from .config import PipelineConfig

logger = logging.getLogger(__name__)


@dataclass
class FileJob:
    """Represents a file to be processed."""
    path: Path
    file_type: str  # 'pdf' or 'image'
    discovered_at: datetime = field(default_factory=datetime.now)
    
    def __hash__(self):
        return hash(self.path)
    
    def __eq__(self, other):
        if isinstance(other, FileJob):
            return self.path == other.path
        return False


class InvoiceFileHandler(FileSystemEventHandler):
    """Handles file system events for invoice files."""
    
    def __init__(self, config: PipelineConfig, job_queue: Queue):
        super().__init__()
        self.config = config
        self.job_queue = job_queue
        self._pending_files: dict[Path, float] = {}  # path -> last_modified_time

    def _is_valid_file(self, path: Path) -> bool:
        """Check if file is a valid invoice file."""
        if not path.is_file():
            return False
        suffix = path.suffix.lower()
        return suffix in self.config.get_all_extensions()

    def _get_file_type(self, path: Path) -> str:
        """Get the file type (pdf or image)."""
        if self.config.is_pdf(path):
            return "pdf"
        elif self.config.is_image(path):
            return "image"
        return "unknown"

    def _add_to_queue(self, path: Path):
        """Add a file to the processing queue after stability check."""
        if not self._is_valid_file(path):
            return
        
        file_type = self._get_file_type(path)
        if file_type == "unknown":
            return

        job = FileJob(path=path, file_type=file_type)
        self.job_queue.put(job)
        logger.info(f"Added to queue: {path} (type: {file_type})")

    def on_created(self, event):
        """Handle file creation events."""
        if event.is_directory:
            return
        
        path = Path(event.src_path)
        
        # Wait for file to be fully written
        self._pending_files[path] = time.time()
        logger.debug(f"File created, waiting for stability: {path}")

    def on_moved(self, event):
        """Handle file move events (e.g., when files are moved into the folder)."""
        if event.is_directory:
            return
        
        dest_path = Path(event.dest_path)
        self._add_to_queue(dest_path)

    def on_modified(self, event):
        """Handle file modification events."""
        if event.is_directory:
            return
        
        path = Path(event.src_path)
        if path in self._pending_files:
            self._pending_files[path] = time.time()

    def check_pending_files(self, stability_seconds: float = 2.0):
        """Check pending files for stability and add stable ones to queue."""
        now = time.time()
        stable_files = []
        
        for path, last_modified in list(self._pending_files.items()):
            if now - last_modified >= stability_seconds:
                if path.exists() and self._is_valid_file(path):
                    stable_files.append(path)
                del self._pending_files[path]

        for path in stable_files:
            self._add_to_queue(path)


class FolderWatcher:
    """Watches folders for new invoice files and queues them for processing."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.job_queue: Queue[FileJob] = Queue()
        self._observer: Optional[Observer] = None
        self._handler: Optional[InvoiceFileHandler] = None
        self._running = False

    def scan_existing_files(self) -> list[FileJob]:
        """Scan watch folders for existing files that haven't been processed."""
        jobs = []
        
        for folder in self.config.watch_folders:
            if not folder.exists():
                logger.warning(f"Watch folder does not exist: {folder}")
                continue
            
            for ext_list, file_type in [
                (self.config.extensions.pdf, "pdf"),
                (self.config.extensions.image, "image"),
            ]:
                for ext in ext_list:
                    for file_path in folder.glob(f"*{ext}"):
                        if file_path.is_file():
                            job = FileJob(path=file_path, file_type=file_type)
                            jobs.append(job)
                            logger.debug(f"Found existing file: {file_path}")
        
        return jobs

    def start(self):
        """Start watching folders for new files."""
        if self._running:
            return

        self._handler = InvoiceFileHandler(self.config, self.job_queue)
        self._observer = Observer()

        for folder in self.config.watch_folders:
            if not folder.exists():
                logger.info(f"Creating watch folder: {folder}")
                folder.mkdir(parents=True, exist_ok=True)
            
            self._observer.schedule(self._handler, str(folder), recursive=False)
            logger.info(f"Watching folder: {folder}")

        self._observer.start()
        self._running = True
        logger.info("Folder watcher started")

    def stop(self):
        """Stop watching folders."""
        if not self._running:
            return

        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

        self._handler = None
        self._running = False
        logger.info("Folder watcher stopped")

    def check_pending(self):
        """Check for pending files that are ready to be queued."""
        if self._handler:
            self._handler.check_pending_files()

    def get_pending_jobs(self, max_count: Optional[int] = None) -> list[FileJob]:
        """Get pending jobs from the queue.
        
        Args:
            max_count: Maximum number of jobs to retrieve (None for all)
            
        Returns:
            List of FileJob objects
        """
        jobs = []
        
        while not self.job_queue.empty():
            if max_count and len(jobs) >= max_count:
                break
            try:
                job = self.job_queue.get_nowait()
                jobs.append(job)
            except:
                break
        
        return jobs

    @property
    def has_pending_jobs(self) -> bool:
        """Check if there are pending jobs in the queue."""
        return not self.job_queue.empty()

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        return False
