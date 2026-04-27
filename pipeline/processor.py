"""Job processor for invoice extraction."""

import json
import subprocess
import shutil
import logging
import tempfile
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from .config import PipelineConfig
from .watcher import FileJob

logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    """Result of processing a file."""
    job: FileJob
    success: bool
    output_folder: Optional[Path] = None
    result_file: Optional[Path] = None
    error_message: Optional[str] = None
    processing_time: float = 0.0


@dataclass
class BatchResult:
    """Result of processing a batch of files."""
    total: int
    successful: int
    failed: int
    results: list[ProcessingResult] = field(default_factory=list)
    processing_time: float = 0.0


class JobProcessor:
    """Processes invoice files using the invoice-processor CLI."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._python_path = config.processor.venv_path / "bin" / "python"
        self._graph_ingester = None

        # Ensure output and archive folders exist
        self.config.output_folder.mkdir(parents=True, exist_ok=True)
        self.config.archive_folder.mkdir(parents=True, exist_ok=True)

    def _ingest_result(self, result_file: Path, output_folder: Path, source_file: Path) -> None:
        """Index a successful extraction result into the graph if enabled."""
        if not self.config.graph.auto_ingest:
            return

        try:
            from .graph_ingester import GraphIngester

            if self._graph_ingester is None:
                self._graph_ingester = GraphIngester(self.config.graph.db_path)
            self._graph_ingester.ingest_json(
                result_file,
                source_file=source_file,
                output_folder=output_folder,
            )
        except Exception as e:
            logger.exception(f"Graph ingestion failed for {result_file}: {e}")

    def _get_output_folder_name(self, file_path: Path) -> str:
        """Generate output folder name from file path."""
        # Use file stem (name without extension) + timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{file_path.stem}_{timestamp}"

    def _create_output_folder(self, file_path: Path) -> Path:
        """Create an output folder for a file."""
        folder_name = self._get_output_folder_name(file_path)
        output_folder = self.config.output_folder / folder_name
        output_folder.mkdir(parents=True, exist_ok=True)
        return output_folder

    def _archive_file(self, file_path: Path, file_type: str) -> Path:
        """Move a processed file to the archive folder."""
        # Create archive subfolder by type and date
        date_folder = datetime.now().strftime("%Y-%m-%d")
        archive_subfolder = self.config.archive_folder / file_type / date_folder
        archive_subfolder.mkdir(parents=True, exist_ok=True)

        # Handle duplicate names
        dest_path = archive_subfolder / file_path.name
        if dest_path.exists():
            timestamp = datetime.now().strftime("%H%M%S")
            dest_path = archive_subfolder / f"{file_path.stem}_{timestamp}{file_path.suffix}"

        shutil.move(str(file_path), str(dest_path))
        logger.info(f"Archived: {file_path} -> {dest_path}")
        return dest_path

    def process_single(self, job: FileJob, server_url: str) -> ProcessingResult:
        """Process a single file.
        
        Args:
            job: The file job to process
            server_url: URL of the llama server
            
        Returns:
            ProcessingResult with status and paths
        """
        start_time = datetime.now()
        
        try:
            # Create output folder for this file
            output_folder = self._create_output_folder(job.path)
            result_file = output_folder / "result.json"
            
            # Copy original file to output folder
            original_copy = output_folder / f"original_{job.path.name}"
            shutil.copy2(str(job.path), str(original_copy))

            # Build command
            cmd = [
                str(self._python_path),
                "-m", "invoice_processor.cli",
                "extract",
                str(job.path),
                "--server",
                "-o", str(result_file),
            ]

            logger.info(f"Processing: {job.path}")
            logger.debug(f"Command: {' '.join(cmd)}")

            # Run the processor
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout per file
                cwd=str(self.config.processor.venv_path.parent),
            )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                logger.error(f"Processing failed for {job.path}: {error_msg}")
                
                # Save error log
                error_file = output_folder / "error.log"
                error_file.write_text(f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}")
                
                return ProcessingResult(
                    job=job,
                    success=False,
                    output_folder=output_folder,
                    error_message=error_msg,
                    processing_time=(datetime.now() - start_time).total_seconds(),
                )

            # Archive the original file
            self._archive_file(job.path, job.file_type)

            # Index the extracted result after the result file has been written.
            self._ingest_result(result_file, output_folder, original_copy)

            logger.info(f"Successfully processed: {job.path} -> {output_folder}")
            
            return ProcessingResult(
                job=job,
                success=True,
                output_folder=output_folder,
                result_file=result_file,
                processing_time=(datetime.now() - start_time).total_seconds(),
            )

        except subprocess.TimeoutExpired:
            error_msg = "Processing timeout exceeded"
            logger.error(f"Timeout processing {job.path}")
            return ProcessingResult(
                job=job,
                success=False,
                error_message=error_msg,
                processing_time=(datetime.now() - start_time).total_seconds(),
            )
        except Exception as e:
            logger.exception(f"Error processing {job.path}")
            return ProcessingResult(
                job=job,
                success=False,
                error_message=str(e),
                processing_time=(datetime.now() - start_time).total_seconds(),
            )

    def process_batch(
        self,
        jobs: list[FileJob],
        server_url: str,
        workers: int = 4,
    ) -> BatchResult:
        """Process a batch of files together.
        
        This uses the batch mode of invoice-processor for efficiency.
        Files are grouped by type (PDF vs image) and processed together.
        
        Args:
            jobs: List of file jobs to process
            server_url: URL of the llama server
            workers: Number of parallel workers
            
        Returns:
            BatchResult with processing results
        """
        start_time = datetime.now()
        results: list[ProcessingResult] = []
        
        # Group jobs by type
        pdf_jobs = [j for j in jobs if j.file_type == "pdf"]
        image_jobs = [j for j in jobs if j.file_type == "image"]
        
        # Process PDFs
        if pdf_jobs:
            pdf_results = self._process_batch_by_type(pdf_jobs, server_url, workers, "pdf")
            results.extend(pdf_results)
        
        # Process images
        if image_jobs:
            image_results = self._process_batch_by_type(image_jobs, server_url, workers, "image")
            results.extend(image_results)
        
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        
        return BatchResult(
            total=len(jobs),
            successful=successful,
            failed=failed,
            results=results,
            processing_time=(datetime.now() - start_time).total_seconds(),
        )

    def _process_batch_by_type(
        self,
        jobs: list[FileJob],
        server_url: str,
        workers: int,
        file_type: str,
    ) -> list[ProcessingResult]:
        """Process a batch of files of the same type.
        
        Creates a temporary directory, copies files there, and runs batch processing.
        """
        results: list[ProcessingResult] = []
        
        if not jobs:
            return results

        with tempfile.TemporaryDirectory(prefix="invoice_batch_") as temp_dir:
            temp_path = Path(temp_dir)
            
            # Map original paths to temp paths
            path_mapping: dict[Path, FileJob] = {}
            
            for job in jobs:
                # Copy file to temp directory
                temp_file = temp_path / job.path.name
                # Handle duplicates
                if temp_file.exists():
                    timestamp = datetime.now().strftime("%H%M%S%f")
                    temp_file = temp_path / f"{job.path.stem}_{timestamp}{job.path.suffix}"
                
                shutil.copy2(str(job.path), str(temp_file))
                path_mapping[temp_file] = job

            # Run batch processing
            batch_result_file = temp_path / "batch_results.json"
            
            cmd = [
                str(self._python_path),
                "-m", "invoice_processor.cli",
                "extract",
                str(temp_path),
                "--batch",
                "-w", str(workers),
                "--server",
                "-o", str(batch_result_file),
            ]

            logger.info(f"Running batch processing for {len(jobs)} {file_type} files")
            logger.debug(f"Command: {' '.join(cmd)}")

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=1800,  # 30 minute timeout for batch
                    cwd=str(self.config.processor.venv_path.parent),
                )

                if result.returncode != 0:
                    error_msg = result.stderr or result.stdout or "Unknown error"
                    logger.error(f"Batch processing failed: {error_msg}")
                    
                    # Mark all jobs as failed
                    for job in jobs:
                        results.append(ProcessingResult(
                            job=job,
                            success=False,
                            error_message=error_msg,
                        ))
                    return results

                # Parse results and distribute to individual output folders
                batch_results = self._parse_batch_results(batch_result_file, path_mapping)
                
                for temp_file, job in path_mapping.items():
                    try:
                        # Create output folder
                        output_folder = self._create_output_folder(job.path)
                        
                        # Copy original to output folder
                        original_copy = output_folder / f"original_{job.path.name}"
                        shutil.copy2(str(job.path), str(original_copy))
                        
                        # Write individual result
                        result_file = output_folder / "result.json"
                        if temp_file.name in batch_results:
                            result_file.write_text(
                                json.dumps(batch_results[temp_file.name], indent=2, ensure_ascii=False)
                            )
                        else:
                            raise ValueError(f"No batch result found for {temp_file.name}")
                        
                        # Archive original
                        self._archive_file(job.path, job.file_type)

                        # Index the extracted result after the result file has been written.
                        self._ingest_result(result_file, output_folder, original_copy)
                        
                        results.append(ProcessingResult(
                            job=job,
                            success=True,
                            output_folder=output_folder,
                            result_file=result_file,
                        ))
                        
                    except Exception as e:
                        logger.exception(f"Error saving results for {job.path}")
                        results.append(ProcessingResult(
                            job=job,
                            success=False,
                            error_message=str(e),
                        ))

            except subprocess.TimeoutExpired:
                logger.error("Batch processing timeout")
                for job in jobs:
                    results.append(ProcessingResult(
                        job=job,
                        success=False,
                        error_message="Batch processing timeout",
                    ))
            except Exception as e:
                logger.exception("Error in batch processing")
                for job in jobs:
                    results.append(ProcessingResult(
                        job=job,
                        success=False,
                        error_message=str(e),
                    ))

        return results

    def _parse_batch_results(
        self,
        result_file: Path,
        path_mapping: dict[Path, FileJob],
    ) -> dict[str, dict]:
        """Parse batch results and map back to original files."""
        results_by_name: dict[str, dict] = {}
        
        if not result_file.exists():
            logger.warning(f"Batch result file not found: {result_file}")
            return results_by_name
        
        try:
            with open(result_file, 'r') as f:
                data = json.load(f)
            
            # Handle both single and multiple results
            if isinstance(data, list):
                for item in data:
                    file_path = item.get("file", "")
                    file_name = Path(file_path).name
                    results_by_name[file_name] = item.get("data", {})
            elif isinstance(data, dict):
                # Single file result
                if "file" in data:
                    file_name = Path(data["file"]).name
                    results_by_name[file_name] = data.get("data", {})
                else:
                    # Assume the whole thing is the result for a single file
                    for temp_path in path_mapping:
                        results_by_name[temp_path.name] = data
                        break
                        
        except Exception as e:
            logger.exception(f"Error parsing batch results: {e}")
        
        return results_by_name
