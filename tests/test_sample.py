"""Tests for sample_script.py"""

import sys
import os

# Add parent directory to path so we can import sample_script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sample_script import main


def test_main_returns_zero():
    """Test that main() returns 0 on success."""
    result = main()
    assert result == 0, "main() should return 0"


def test_output_file_created(tmp_path, monkeypatch):
    """Test that output.txt is created."""
    # Change to temp directory
    monkeypatch.chdir(tmp_path)
    
    # Run main
    result = main()
    
    # Check output file exists
    output_file = tmp_path / "output.txt"
    assert output_file.exists(), "output.txt should be created"
    
    # Check content
    content = output_file.read_text()
    assert "Sum=15" in content, "output.txt should contain Sum=15"
