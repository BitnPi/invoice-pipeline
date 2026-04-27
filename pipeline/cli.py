#!/usr/bin/env python3
"""CLI entry point for the invoice pipeline."""

import argparse
import os
import sys
import logging
from pathlib import Path
from typing import Optional

from .config import load_config, PipelineConfig
from .main import Pipeline, setup_logging
from .watcher import FileJob

logger = logging.getLogger(__name__)


def _daemon_lock_path(config: PipelineConfig) -> Path:
    return Path(str(config.graph.db_path) + ".daemon.lock")


def _is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def _acquire_daemon_lock(config: PipelineConfig) -> Optional[Path]:
    lock = _daemon_lock_path(config)
    lock.parent.mkdir(parents=True, exist_ok=True)
    if lock.exists():
        try:
            existing_pid = int(lock.read_text().strip())
        except (ValueError, OSError):
            existing_pid = None
        if existing_pid and _is_pid_alive(existing_pid):
            print(
                f"Error: another invoice-pipeline daemon (pid {existing_pid}) "
                f"already holds the graph lock at {lock}.",
                file=sys.stderr,
            )
            sys.exit(1)
    lock.write_text(str(os.getpid()))
    return lock


def _release_daemon_lock(lock: Optional[Path]) -> None:
    if not lock:
        return
    try:
        if lock.exists() and lock.read_text().strip() == str(os.getpid()):
            lock.unlink()
    except OSError:
        pass


def cmd_run(args, config: PipelineConfig):
    """Run the pipeline (one-shot or daemon mode)."""
    pipeline = Pipeline(config)
    daemon_lock: Optional[Path] = None
    if args.daemon and config.graph.auto_ingest:
        daemon_lock = _acquire_daemon_lock(config)

    try:
        if args.daemon:
            pipeline.run_daemon()
        else:
            result = pipeline.run_once()
            print(f"\nProcessing complete:")
            print(f"  Total:      {result.total}")
            print(f"  Successful: {result.successful}")
            print(f"  Failed:     {result.failed}")
            print(f"  Time:       {result.processing_time:.1f}s")

            if result.failed > 0:
                sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        _release_daemon_lock(daemon_lock)
        pipeline.cleanup()


def cmd_process(args, config: PipelineConfig):
    """Process specific files."""
    files = [Path(f) for f in args.files]
    
    # Validate files exist
    for f in files:
        if not f.exists():
            print(f"Error: File not found: {f}", file=sys.stderr)
            sys.exit(1)
    
    # Create jobs
    jobs = []
    for f in files:
        if config.is_pdf(f):
            file_type = "pdf"
        elif config.is_image(f):
            file_type = "image"
        else:
            print(f"Warning: Unsupported file type: {f}", file=sys.stderr)
            continue
        
        jobs.append(FileJob(path=f.resolve(), file_type=file_type))
    
    if not jobs:
        print("No valid files to process", file=sys.stderr)
        sys.exit(1)
    
    pipeline = Pipeline(config)
    
    try:
        result = pipeline.run_once(jobs)
        
        print(f"\nProcessing complete:")
        for r in result.results:
            status = "✓" if r.success else "✗"
            output = r.output_folder if r.success else r.error_message
            print(f"  {status} {r.job.path.name} -> {output}")
        
        print(f"\nTotal: {result.successful}/{result.total} successful in {result.processing_time:.1f}s")
        
        if result.failed > 0:
            sys.exit(1)
    finally:
        pipeline.cleanup()


def cmd_status(args, config: PipelineConfig):
    """Show pipeline status."""
    from .server import LlamaServerManager
    from .watcher import FolderWatcher
    
    print("Invoice Pipeline Status")
    print("=" * 50)
    
    # Check watch folders
    print("\nWatch Folders:")
    for folder in config.watch_folders:
        exists = "✓" if folder.exists() else "✗"
        file_count = len(list(folder.glob("*"))) if folder.exists() else 0
        print(f"  {exists} {folder} ({file_count} files)")
    
    # Check output/archive folders
    print("\nOutput Folders:")
    for name, folder in [("Archive", config.archive_folder), ("Output", config.output_folder)]:
        exists = "✓" if folder.exists() else "✗"
        print(f"  {exists} {name}: {folder}")
    
    # Check server
    print("\nServer:")
    server_manager = LlamaServerManager(config.server)
    status = server_manager.get_status()
    if status.running:
        print(f"  ✓ Running (healthy: {status.healthy}, slots: {status.slots_available})")
    else:
        print(f"  ○ Not running")
    
    # Check model files
    print("\nModel Files:")
    for name, path in [("Model", config.server.model), ("MMProj", config.server.mmproj)]:
        exists = "✓" if path.exists() else "✗"
        print(f"  {exists} {name}: {path}")
    
    # Check processor
    print("\nProcessor:")
    python_path = config.processor.venv_path / "bin" / "python"
    exists = "✓" if python_path.exists() else "✗"
    print(f"  {exists} Python: {python_path}")
    
    # Scan for pending files
    print("\nPending Files:")
    watcher = FolderWatcher(config)
    jobs = watcher.scan_existing_files()
    if jobs:
        for job in jobs[:10]:  # Show first 10
            print(f"  - {job.path} ({job.file_type})")
        if len(jobs) > 10:
            print(f"  ... and {len(jobs) - 10} more")
    else:
        print("  No pending files")


def cmd_init(args, config: PipelineConfig):
    """Initialize folder structure."""
    print("Creating folder structure...")
    
    folders_to_create = [
        *config.watch_folders,
        config.archive_folder,
        config.output_folder,
    ]
    
    if config.logging.file:
        folders_to_create.append(config.logging.file.parent)
    
    for folder in folders_to_create:
        folder.mkdir(parents=True, exist_ok=True)
        print(f"  ✓ {folder}")
    
    print("\nFolder structure initialized!")
    print("\nDrop invoice files into watch folders to process them:")
    for folder in config.watch_folders:
        print(f"  {folder}")


def cmd_graph(args, config: PipelineConfig):
    """Ingest processed results into the Kuzu Knowledge Graph."""
    from .graph_ingester import GraphIngester

    print("Graph Ingestion Pipeline")
    print("=" * 50)

    lock = _daemon_lock_path(config)
    if lock.exists():
        try:
            holder_pid = int(lock.read_text().strip())
        except (ValueError, OSError):
            holder_pid = None
        if holder_pid and _is_pid_alive(holder_pid):
            print(
                f"Error: invoice-pipeline daemon (pid {holder_pid}) is currently writing "
                f"to the graph DB at {config.graph.db_path}.",
                file=sys.stderr,
            )
            print(
                "Stop the daemon before running 'graph' to avoid Kuzu writer-lock conflicts.",
                file=sys.stderr,
            )
            sys.exit(1)

    db_path = str(config.graph.db_path)
    ingester = GraphIngester(db_path=db_path)
    
    output_folder = config.output_folder
    if not output_folder.exists():
        print(f"Error: Output folder {output_folder} does not exist.")
        sys.exit(1)
        
    print(f"\nStarting ingestion from {output_folder}...")
    count = ingester.ingest_directory(output_folder)
    print(f"\nDone! Ingested {count} invoice(s) into Kuzu graph at {db_path}")

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Invoice Processing Pipeline - Automated invoice extraction"
    )
    parser.add_argument(
        "-c", "--config",
        type=Path,
        help="Path to config file",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run the pipeline")
    run_parser.add_argument(
        "-d", "--daemon",
        action="store_true",
        help="Run as daemon (continuous watching)",
    )
    
    # Process command
    process_parser = subparsers.add_parser("process", help="Process specific files")
    process_parser.add_argument(
        "files",
        nargs="+",
        help="Files to process",
    )
    
    # Status command
    subparsers.add_parser("status", help="Show pipeline status")
    
    # Init command
    subparsers.add_parser("init", help="Initialize folder structure")
    
    # Graph command
    subparsers.add_parser("graph", help="Ingest processed results into Knowledge Graph")
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        if args.command == "init":
            # For init, create a default config
            print("No config file found. Using defaults...")
            from .config import (
                PipelineConfig, ServerConfig, ProcessorConfig,
                WatcherConfig, LoggingConfig, ExtensionsConfig, GraphConfig, expand_path
            )
            config = PipelineConfig(
                watch_folders=[
                    expand_path("~/invoice-inbox/pdf"),
                    expand_path("~/invoice-inbox/images"),
                ],
                archive_folder=expand_path("~/invoice-inbox/archive"),
                output_folder=expand_path("~/invoice-inbox/output"),
                server=ServerConfig(
                    binary=expand_path("~/llama.cpp/build/bin/llama-server"),
                    model=expand_path("~/Projects/models/Qwen3-VL-4B-Instruct-Q4_K_M.gguf"),
                    mmproj=expand_path("~/Projects/models/mmproj-Qwen3-VL-4B-Instruct-F16.gguf"),
                ),
                processor=ProcessorConfig(
                    venv_path=expand_path("~/Projects/invoice-processor/.venv"),
                ),
                watcher=WatcherConfig(),
                logging=LoggingConfig(
                    file=expand_path("~/invoice-inbox/logs/pipeline.log"),
                ),
                extensions=ExtensionsConfig(),
                graph=GraphConfig(
                    db_path=expand_path("~/invoice-inbox/graph_db"),
                    auto_ingest=True,
                ),
            )
        else:
            print(f"Error: {e}", file=sys.stderr)
            print("Run 'invoice-pipeline init' to create the folder structure.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Override log level if verbose
    if args.verbose:
        config.logging.level = "DEBUG"
    
    # Setup logging
    setup_logging(config)
    
    # Dispatch command
    if args.command == "run":
        cmd_run(args, config)
    elif args.command == "process":
        cmd_process(args, config)
    elif args.command == "status":
        cmd_status(args, config)
    elif args.command == "init":
        cmd_init(args, config)
    elif args.command == "graph":
        cmd_graph(args, config)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
