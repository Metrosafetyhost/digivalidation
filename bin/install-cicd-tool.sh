#!/bin/bash

# Check if Homebrew is installed
if command -v brew &> /dev/null; then
    echo "🍺 Homebrew detected. Installing Babashka and uv (python package tool) using Homebrew..."
    brew install borkdude/brew/babashka uv
else
    echo "🚀 Homebrew not found. Installing Babashka using official script..."
    bash < <(curl -s https://raw.githubusercontent.com/babashka/babashka/master/install)
fi

# Verify installation
if command -v bb &> /dev/null; then
    echo "✅ Babashka installed successfully!"
    bb --version
else
    echo "❌ Installation failed. Please check for errors."
    exit 1
fi
