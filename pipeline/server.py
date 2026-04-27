"""Llama server lifecycle management."""

import subprocess
import time
import signal
import logging
import requests
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from .config import ServerConfig

logger = logging.getLogger(__name__)


@dataclass
class ServerStatus:
    """Server status information."""
    running: bool
    healthy: bool
    pid: Optional[int] = None
    slots_available: int = 0


class LlamaServerManager:
    """Manages the lifecycle of llama-server."""

    def __init__(self, config: ServerConfig):
        self.config = config
        self.process: Optional[subprocess.Popen] = None
        self._started_by_us = False

    @property
    def health_url(self) -> str:
        """Get the health check URL."""
        return f"http://{self.config.host}:{self.config.port}/health"

    @property
    def base_url(self) -> str:
        """Get the base server URL."""
        return f"http://{self.config.host}:{self.config.port}"

    def is_running(self) -> bool:
        """Check if the server is running (by us or externally)."""
        try:
            response = requests.get(self.health_url, timeout=2)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def get_status(self) -> ServerStatus:
        """Get detailed server status."""
        try:
            response = requests.get(self.health_url, timeout=2)
            if response.status_code == 200:
                data = response.json()
                return ServerStatus(
                    running=True,
                    healthy=data.get("status") == "ok",
                    pid=self.process.pid if self.process else None,
                    slots_available=data.get("slots_idle", 0),
                )
        except requests.RequestException:
            pass
        
        return ServerStatus(
            running=False,
            healthy=False,
            pid=self.process.pid if self.process else None,
        )

    def start(self, num_workers: int = 4) -> bool:
        """Start the llama server if not already running.
        
        Args:
            num_workers: Number of parallel slots to configure
            
        Returns:
            True if server is running (started or already running), False otherwise
        """
        # Check if already running externally
        if self.is_running():
            logger.info("Server already running externally")
            self._started_by_us = False
            return True

        # Validate binary exists
        if not self.config.binary.exists():
            logger.error(f"llama-server binary not found: {self.config.binary}")
            return False

        if not self.config.model.exists():
            logger.error(f"Model not found: {self.config.model}")
            return False

        if not self.config.mmproj.exists():
            logger.error(f"MMProj not found: {self.config.mmproj}")
            return False

        # Build command
        cmd = [
            str(self.config.binary),
            "-m", str(self.config.model),
            "--mmproj", str(self.config.mmproj),
            "--port", str(self.config.port),
            "--host", self.config.host,
            "-ngl", str(self.config.gpu_layers),
            "-c", str(self.config.context_size),
            "-np", str(num_workers),
        ]

        logger.info(f"Starting llama-server: {' '.join(cmd)}")

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_IGN),
            )
            self._started_by_us = True
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            return False

        # Wait for server to be ready
        logger.info("Waiting for server to be ready...")
        start_time = time.time()
        
        while time.time() - start_time < self.config.startup_timeout:
            # Check if process died
            if self.process.poll() is not None:
                stderr = self.process.stderr.read().decode() if self.process.stderr else ""
                logger.error(f"Server process died during startup: {stderr}")
                self.process = None
                self._started_by_us = False
                return False

            try:
                response = requests.get(self.health_url, timeout=2)
                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status", "")
                    if status == "ok":
                        logger.info(f"Server ready (PID: {self.process.pid})")
                        return True
                    elif status == "loading model":
                        logger.debug("Server loading model...")
            except requests.RequestException:
                pass
            
            time.sleep(1)

        logger.error(f"Timeout waiting for server to start after {self.config.startup_timeout}s")
        self.stop()
        return False

    def stop(self) -> bool:
        """Stop the server if we started it.
        
        Returns:
            True if stopped successfully, False otherwise
        """
        if not self._started_by_us:
            logger.info("Server was not started by us, not stopping")
            return True

        if self.process is None:
            logger.info("No server process to stop")
            return True

        logger.info(f"Stopping server (PID: {self.process.pid})...")

        try:
            # Try graceful shutdown first
            self.process.terminate()
            
            try:
                self.process.wait(timeout=10)
                logger.info("Server stopped gracefully")
            except subprocess.TimeoutExpired:
                logger.warning("Server did not stop gracefully, killing...")
                self.process.kill()
                self.process.wait(timeout=5)
                logger.info("Server killed")

            self.process = None
            self._started_by_us = False
            return True

        except Exception as e:
            logger.error(f"Error stopping server: {e}")
            return False

    def wait_for_slot(self, timeout: int = 60) -> bool:
        """Wait for an available slot on the server.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if slot available, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self.get_status()
            if status.healthy and status.slots_available > 0:
                return True
            time.sleep(0.5)
        
        return False

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures server is stopped."""
        self.stop()
        return False
