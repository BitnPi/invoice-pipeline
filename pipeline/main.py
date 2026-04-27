"""Main pipeline orchestrator."""

import signal
import time
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

from .config import PipelineConfig, load_config
from .server import LlamaServerManager
from .watcher import FolderWatcher, FileJob
from .processor import JobProcessor, BatchResult

logger = logging.getLogger(__name__)


class Pipeline:
    """Main pipeline orchestrator that coordinates all components."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.server_manager = LlamaServerManager(config.server)
        self.watcher = FolderWatcher(config)
        self.processor = JobProcessor(config)
        
        self._running = False
        self._shutdown_requested = False

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self._shutdown_requested = True
            self._running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def run_once(self, jobs: Optional[list[FileJob]] = None) -> BatchResult:
        """Run the pipeline once for the given jobs or existing files.
        
        This is useful for one-shot processing or testing.
        
        Args:
            jobs: Optional list of jobs to process. If None, scans watch folders.
            
        Returns:
            BatchResult with processing results
        """
        # Get jobs if not provided
        if jobs is None:
            jobs = self.watcher.scan_existing_files()
        
        if not jobs:
            logger.info("No files to process")
            return BatchResult(total=0, successful=0, failed=0)

        logger.info(f"Processing {len(jobs)} file(s)...")

        # Start server
        num_workers = min(self.config.processor.workers, len(jobs))
        if not self.server_manager.start(num_workers):
            logger.error("Failed to start server")
            return BatchResult(
                total=len(jobs),
                successful=0,
                failed=len(jobs),
                results=[],
            )

        try:
            # Process files
            result = self.processor.process_batch(
                jobs,
                self.server_manager.base_url,
                workers=num_workers,
            )
            
            logger.info(
                f"Batch complete: {result.successful}/{result.total} successful "
                f"in {result.processing_time:.1f}s"
            )
            
            return result

        finally:
            # Stop server
            self.server_manager.stop()

    def run_daemon(self):
        """Run the pipeline as a daemon, watching for new files continuously.
        
        The pipeline will:
        1. Watch configured folders for new files
        2. Collect files into batches
        3. Start the server when there are files to process
        4. Process the batch
        5. Stop the server when done
        6. Repeat
        """
        self.setup_signal_handlers()
        self._running = True
        
        logger.info("=" * 60)
        logger.info("Invoice Processing Pipeline Started")
        logger.info("=" * 60)
        logger.info(f"Watch folders: {[str(f) for f in self.config.watch_folders]}")
        logger.info(f"Archive folder: {self.config.archive_folder}")
        logger.info(f"Output folder: {self.config.output_folder}")
        logger.info("=" * 60)
        
        # First, process any existing files
        existing_jobs = self.watcher.scan_existing_files()
        if existing_jobs:
            logger.info(f"Found {len(existing_jobs)} existing file(s) to process")
            self.run_once(existing_jobs)

        # Start watching for new files
        self.watcher.start()
        
        pending_jobs: list[FileJob] = []
        last_job_time: Optional[datetime] = None

        try:
            while self._running and not self._shutdown_requested:
                # Check for file stability
                self.watcher.check_pending()
                
                # Get new jobs from watcher
                new_jobs = self.watcher.get_pending_jobs()
                if new_jobs:
                    pending_jobs.extend(new_jobs)
                    last_job_time = datetime.now()
                    logger.info(f"Queued {len(new_jobs)} new file(s), total pending: {len(pending_jobs)}")

                # Decide if we should process
                should_process = False
                
                if pending_jobs:
                    # Check if we have a full batch
                    if len(pending_jobs) >= self.config.watcher.max_batch_size:
                        should_process = True
                        logger.info("Batch size reached, starting processing")
                    # Check if batch delay has passed
                    elif last_job_time:
                        delay = (datetime.now() - last_job_time).total_seconds()
                        if delay >= self.config.watcher.batch_delay:
                            should_process = True
                            logger.info(f"Batch delay ({delay:.1f}s) reached, starting processing")

                if should_process and pending_jobs:
                    # Take up to max_batch_size jobs
                    batch = pending_jobs[:self.config.watcher.max_batch_size]
                    pending_jobs = pending_jobs[self.config.watcher.max_batch_size:]
                    
                    # Process the batch
                    result = self.run_once(batch)
                    
                    # Log summary
                    for r in result.results:
                        if r.success:
                            logger.info(f"✓ {r.job.path.name} -> {r.output_folder}")
                        else:
                            logger.error(f"✗ {r.job.path.name}: {r.error_message}")
                    
                    last_job_time = None

                # Sleep before next check
                time.sleep(self.config.watcher.poll_interval)

        except Exception as e:
            logger.exception(f"Pipeline error: {e}")
            raise
        finally:
            self.watcher.stop()
            self.server_manager.stop()
            logger.info("Pipeline stopped")

    def cleanup(self):
        """Cleanup resources."""
        self.watcher.stop()
        self.server_manager.stop()


def setup_logging(config: PipelineConfig):
    """Setup logging based on configuration."""
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    log_level = getattr(logging, config.logging.level.upper(), logging.INFO)
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if config.logging.file:
        config.logging.file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(config.logging.file))
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
    )


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Invoice Processing Pipeline")
    parser.add_argument(
        "-c", "--config",
        type=Path,
        help="Path to config file",
    )
    parser.add_argument(
        "-d", "--daemon",
        action="store_true",
        help="Run as daemon (continuous watching)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Override log level if verbose
    if args.verbose:
        config.logging.level = "DEBUG"
    
    # Setup logging
    setup_logging(config)
    
    # Create and run pipeline
    pipeline = Pipeline(config)
    
    try:
        if args.daemon:
            pipeline.run_daemon()
        else:
            result = pipeline.run_once()
            if result.failed > 0:
                sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Pipeline error: {e}")
        sys.exit(1)
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()
