#!/bin/bash
set -e

echo "🔧 Setting up pdf-library..."

# Check for Homebrew
if ! command -v brew &> /dev/null; then
    echo "❌ Homebrew not found. Install from https://brew.sh"
    exit 1
fi

# Install Ollama if not present
if ! command -v ollama &> /dev/null; then
    echo "📦 Installing Ollama..."
    brew install ollama
fi

# Start Ollama service if not running
if ! pgrep -x "ollama" > /dev/null; then
    echo "🚀 Starting Ollama service..."
    ollama serve &
    sleep 3
fi

# Pull embedding model
echo "📥 Pulling mxbai-embed-large model (this may take a few minutes)..."
ollama pull mxbai-embed-large

# Install bun if not present
if ! command -v bun &> /dev/null; then
    echo "📦 Installing Bun..."
    curl -fsSL https://bun.sh/install | bash
fi

# Install dependencies
echo "📦 Installing npm dependencies..."
bun install

# Create library directory
LIBRARY_DIR="${PDF_LIBRARY_PATH:-$HOME/Documents/.pdf-library}"
mkdir -p "$LIBRARY_DIR"
echo "📁 Library directory: $LIBRARY_DIR"

echo ""
echo "✅ Setup complete!"
echo ""
echo "Usage:"
echo "  bun run dev add /path/to/file.pdf"
echo "  bun run dev search 'context engineering'"
echo "  bun run dev list"
echo ""
echo "Or install globally:"
echo "  bun link"
echo "  pdf-library add /path/to/file.pdf"
