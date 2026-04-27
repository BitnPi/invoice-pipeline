"""Configuration loader for the invoice pipeline."""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
import yaml


@dataclass
class ServerConfig:
    """Llama server configuration."""
    binary: Path
    model: Path
    mmproj: Path
    port: int = 8081
    host: str = "127.0.0.1"
    gpu_layers: int = 99
    context_size: int = 32768
    parallel_slots: int = 4
    startup_timeout: int = 120


@dataclass
class ProcessorConfig:
    """Invoice processor configuration."""
    venv_path: Path
    workers: int = 4


@dataclass
class WatcherConfig:
    """Folder watcher configuration."""
    poll_interval: int = 5
    batch_delay: int = 10
    max_batch_size: int = 50


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    file: Optional[Path] = None


@dataclass
class ExtensionsConfig:
    """File extension configuration."""
    pdf: list[str] = field(default_factory=lambda: [".pdf"])
    image: list[str] = field(default_factory=lambda: [".png", ".jpg", ".jpeg", ".webp", ".tiff", ".bmp"])


@dataclass
class GraphConfig:
    """Knowledge graph configuration."""
    db_path: Path
    auto_ingest: bool = True


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    watch_folders: list[Path]
    archive_folder: Path
    output_folder: Path
    server: ServerConfig
    processor: ProcessorConfig
    watcher: WatcherConfig
    logging: LoggingConfig
    extensions: ExtensionsConfig
    graph: GraphConfig

    def get_all_extensions(self) -> list[str]:
        """Get all supported file extensions."""
        return self.extensions.pdf + self.extensions.image

    def is_pdf(self, path: Path) -> bool:
        """Check if a file is a PDF."""
        return path.suffix.lower() in self.extensions.pdf

    def is_image(self, path: Path) -> bool:
        """Check if a file is an image."""
        return path.suffix.lower() in self.extensions.image


def expand_path(path_str: str) -> Path:
    """Expand ~ and environment variables in path."""
    return Path(os.path.expanduser(os.path.expandvars(path_str)))


def load_config(config_path: Optional[Path] = None) -> PipelineConfig:
    """Load configuration from YAML file."""
    if config_path is None:
        # Look for config in default locations
        default_locations = [
            Path.cwd() / "config.yaml",
            Path(__file__).parent.parent / "config.yaml",
            Path.home() / ".config" / "invoice-pipeline" / "config.yaml",
        ]
        for loc in default_locations:
            if loc.exists():
                config_path = loc
                break
        else:
            raise FileNotFoundError(
                "No config file found. Please create config.yaml or specify with --config"
            )

    with open(config_path, 'r') as f:
        data = yaml.safe_load(f)

    # Parse watch folders
    watch_folders = [expand_path(p) for p in data.get("watch_folders", [])]
    
    # Parse archive and output folders
    archive_folder = expand_path(data.get("archive_folder", "~/invoice-inbox/archive"))
    output_folder = expand_path(data.get("output_folder", "~/invoice-inbox/output"))
    
    # Parse server config
    server_data = data.get("server", {})
    server = ServerConfig(
        binary=expand_path(server_data.get("binary", "~/llama.cpp/build/bin/llama-server")),
        model=expand_path(server_data.get("model", "")),
        mmproj=expand_path(server_data.get("mmproj", "")),
        port=server_data.get("port", 8081),
        host=server_data.get("host", "127.0.0.1"),
        gpu_layers=server_data.get("gpu_layers", 99),
        context_size=server_data.get("context_size", 32768),
        parallel_slots=server_data.get("parallel_slots", 4),
        startup_timeout=server_data.get("startup_timeout", 120),
    )
    
    # Parse processor config
    processor_data = data.get("processor", {})
    processor = ProcessorConfig(
        venv_path=expand_path(processor_data.get("venv_path", "~/Projects/invoice-processor/.venv")),
        workers=processor_data.get("workers", 4),
    )
    
    # Parse watcher config
    watcher_data = data.get("watcher", {})
    watcher = WatcherConfig(
        poll_interval=watcher_data.get("poll_interval", 5),
        batch_delay=watcher_data.get("batch_delay", 10),
        max_batch_size=watcher_data.get("max_batch_size", 50),
    )
    
    # Parse logging config
    logging_data = data.get("logging", {})
    logging_config = LoggingConfig(
        level=logging_data.get("level", "INFO"),
        file=expand_path(logging_data["file"]) if logging_data.get("file") else None,
    )
    
    # Parse extensions
    ext_data = data.get("extensions", {})
    extensions = ExtensionsConfig(
        pdf=ext_data.get("pdf", [".pdf"]),
        image=ext_data.get("image", [".png", ".jpg", ".jpeg", ".webp", ".tiff", ".bmp"]),
    )

    # Parse graph config
    graph_data = data.get("graph", {})
    graph = GraphConfig(
        db_path=expand_path(graph_data.get("db_path", "~/invoice-inbox/graph_db")),
        auto_ingest=graph_data.get("auto_ingest", True),
    )
    
    return PipelineConfig(
        watch_folders=watch_folders,
        archive_folder=archive_folder,
        output_folder=output_folder,
        server=server,
        processor=processor,
        watcher=watcher,
        logging=logging_config,
        extensions=extensions,
        graph=graph,
    )
