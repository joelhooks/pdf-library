/**
 * IngestProgress - TUI component for batch PDF/Markdown ingestion
 *
 * Displays real-time progress for:
 * - File discovery
 * - Chunking
 * - Embedding generation
 * - Overall progress
 */

import * as React from "react";
import { render, Box, Text, useApp, useInput } from "ink";
import Spinner from "ink-spinner";

/** Status of a single file being processed */
export interface FileStatus {
  path: string;
  filename: string;
  status: "pending" | "chunking" | "embedding" | "done" | "error";
  chunks?: number;
  error?: string;
}

/** Overall ingest progress state */
export interface IngestState {
  phase: "discovering" | "processing" | "done" | "error";
  totalFiles: number;
  processedFiles: number;
  currentFile?: FileStatus;
  recentFiles: FileStatus[];
  errors: FileStatus[];
  startTime: number;
  endTime?: number;
}

/** Props for the IngestProgress component */
interface IngestProgressProps {
  state: IngestState;
  onCancel?: () => void;
}

/**
 * Format duration in human-readable form
 */
function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) {
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
  } else if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  } else {
    return `${seconds}s`;
  }
}

/**
 * Calculate ETA based on current progress
 */
function calculateETA(state: IngestState): string {
  if (state.processedFiles === 0) return "calculating...";

  const elapsed = Date.now() - state.startTime;
  const avgTimePerFile = elapsed / state.processedFiles;
  const remaining = state.totalFiles - state.processedFiles;
  const etaMs = avgTimePerFile * remaining;

  return formatDuration(etaMs);
}

/**
 * Progress bar component
 */
function ProgressBar({
  percent,
  width = 40,
}: {
  percent: number;
  width?: number;
}) {
  const filled = Math.round((percent / 100) * width);
  const empty = width - filled;

  return (
    <Text>
      <Text color="green">{"█".repeat(filled)}</Text>
      <Text color="gray">{"░".repeat(empty)}</Text>
      <Text> {percent.toFixed(1)}%</Text>
    </Text>
  );
}

/**
 * Status icon based on file status
 */
function StatusIcon({ status }: { status: FileStatus["status"] }) {
  switch (status) {
    case "pending":
      return <Text color="gray">○</Text>;
    case "chunking":
      return (
        <Text color="yellow">
          <Spinner type="dots" />
        </Text>
      );
    case "embedding":
      return (
        <Text color="blue">
          <Spinner type="dots" />
        </Text>
      );
    case "done":
      return <Text color="green">✓</Text>;
    case "error":
      return <Text color="red">✗</Text>;
  }
}

/**
 * Main IngestProgress TUI component
 */
export function IngestProgress({ state, onCancel }: IngestProgressProps) {
  const { exit } = useApp();

  // Handle keyboard input
  useInput((input, key) => {
    if (input === "q" || (key.ctrl && input === "c")) {
      onCancel?.();
      exit();
    }
  });

  const percent =
    state.totalFiles > 0 ? (state.processedFiles / state.totalFiles) * 100 : 0;

  const elapsed = formatDuration(Date.now() - state.startTime);
  const eta = state.phase === "done" ? "-" : calculateETA(state);

  return (
    <Box flexDirection="column" padding={1}>
      {/* Header */}
      <Box marginBottom={1}>
        <Text bold color="cyan">
          ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        </Text>
      </Box>
      <Box marginBottom={1}>
        <Text bold color="cyan">
          ┃ PDF Brain - Batch Ingest ┃
        </Text>
      </Box>
      <Box marginBottom={1}>
        <Text bold color="cyan">
          ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
        </Text>
      </Box>

      {/* Phase indicator */}
      <Box marginBottom={1}>
        <Text>
          Phase:{" "}
          {state.phase === "discovering" && (
            <Text color="yellow">
              <Spinner type="dots" /> Discovering files...
            </Text>
          )}
          {state.phase === "processing" && (
            <Text color="blue">Processing files</Text>
          )}
          {state.phase === "done" && <Text color="green">Complete!</Text>}
          {state.phase === "error" && <Text color="red">Error</Text>}
        </Text>
      </Box>

      {/* Progress bar */}
      <Box marginBottom={1}>
        <Text>Progress: </Text>
        <ProgressBar percent={percent} />
      </Box>

      {/* Stats */}
      <Box marginBottom={1}>
        <Text>
          Files: <Text color="green">{state.processedFiles}</Text>
          <Text color="gray"> / </Text>
          <Text>{state.totalFiles}</Text>
          <Text color="gray"> | </Text>
          Elapsed: <Text color="cyan">{elapsed}</Text>
          <Text color="gray"> | </Text>
          ETA: <Text color="yellow">{eta}</Text>
        </Text>
      </Box>

      {/* Current file */}
      {state.currentFile && (
        <Box marginBottom={1}>
          <Text>
            Current: <StatusIcon status={state.currentFile.status} />{" "}
            <Text color="white">{state.currentFile.filename}</Text>
            {state.currentFile.status === "chunking" && (
              <Text color="gray"> (chunking...)</Text>
            )}
            {state.currentFile.status === "embedding" && (
              <Text color="gray"> (embedding...)</Text>
            )}
            {state.currentFile.chunks && (
              <Text color="gray"> ({state.currentFile.chunks} chunks)</Text>
            )}
          </Text>
        </Box>
      )}

      {/* Recent files */}
      {state.recentFiles.length > 0 && (
        <Box flexDirection="column" marginBottom={1}>
          <Text color="gray">Recent:</Text>
          {state.recentFiles.slice(-5).map((file) => (
            <Box key={file.path} marginLeft={2}>
              <StatusIcon status={file.status} />
              <Text> {file.filename}</Text>
              {file.chunks && <Text color="gray"> ({file.chunks} chunks)</Text>}
              {file.error && <Text color="red"> - {file.error}</Text>}
            </Box>
          ))}
        </Box>
      )}

      {/* Errors summary */}
      {state.errors.length > 0 && (
        <Box flexDirection="column" marginBottom={1}>
          <Text color="red">Errors ({state.errors.length}):</Text>
          {state.errors.slice(-3).map((file) => (
            <Box key={file.path} marginLeft={2}>
              <Text color="red">
                ✗ {file.filename}: {file.error}
              </Text>
            </Box>
          ))}
          {state.errors.length > 3 && (
            <Box marginLeft={2}>
              <Text color="gray">... and {state.errors.length - 3} more</Text>
            </Box>
          )}
        </Box>
      )}

      {/* Footer */}
      <Box marginTop={1}>
        <Text color="gray">Press 'q' to cancel</Text>
      </Box>
    </Box>
  );
}

/**
 * Render the IngestProgress TUI
 * Returns controls for updating state and cleanup
 */
export function renderIngestProgress(initialState: IngestState) {
  let currentState = initialState;
  let rerender: ((node: React.ReactNode) => void) | null = null;
  let cancelled = false;

  const handleCancel = () => {
    cancelled = true;
  };

  const {
    rerender: _rerender,
    unmount,
    clear,
  } = render(<IngestProgress state={currentState} onCancel={handleCancel} />);
  rerender = _rerender;

  return {
    /** Update the display state */
    update(newState: Partial<IngestState>) {
      currentState = { ...currentState, ...newState };
      rerender?.(
        <IngestProgress state={currentState} onCancel={handleCancel} />
      );
    },

    /** Check if user cancelled */
    isCancelled() {
      return cancelled;
    },

    /** Clean up the TUI */
    cleanup() {
      clear();
      unmount();
    },

    /** Get current state */
    getState() {
      return currentState;
    },
  };
}

/** Create initial ingest state */
export function createInitialState(): IngestState {
  return {
    phase: "discovering",
    totalFiles: 0,
    processedFiles: 0,
    recentFiles: [],
    errors: [],
    startTime: Date.now(),
  };
}
