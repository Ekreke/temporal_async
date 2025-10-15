# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Go-based project that converts n8n workflows to Temporal workflows. It implements a bridge between n8n's JSON-based workflow definitions and Temporal's durable execution system.

## Architecture

The project follows a standard Temporal workflow architecture with three main components:

- **Workflows** (`workflow/`): Define the orchestration logic and execution flow
- **Activities** (`activity/`): Implement individual atomic operations that can be retried
- **Worker** (`worker/`): Hosts the workflows and activities, processes tasks from Temporal

### Key Components

- **N8NConvertedWorkflow**: Main workflow that demonstrates a converted n8n workflow with domain resolution, Python execution, conditional branching, and switch nodes
- **GenericWorkflow**: Placeholder for a generic n8n-to-Temporal conversion workflow
- **Activity Types**: DomainResolveActivity, PythonCodeActivity, CustomNodeActivity, ConditionCheckActivity, SwitchNodeActivity

## Common Commands

### Build and Run
```bash
# Build the worker
go build ./worker

# Run the worker (requires Temporal server running on localhost:7233)
go run ./worker
```

### Testing
```bash
# Run all tests
go test ./...

# Run tests for specific package
go test ./workflow
go test ./activity
```

### Development
```bash
# Format code
go fmt ./...

# Vet for potential issues
go vet ./...

# Tidy dependencies
go mod tidy

# Add new dependencies
go get <package>
```

## Development Notes

- The project uses Go 1.24.6 and Temporal SDK v1.37.0
- Temporal server is expected to run on `localhost:7233`
- Activities are designed to be idempotent and retry-safe
- The workflow demonstrates parallel execution using Temporal's Future pattern
- All activities accept and return `map[string]interface{}` for flexibility with n8n's data format

## Workflow Configuration

The worker is configured to listen to the "n8n-conversion-queue" task queue. Workflow execution supports:
- 24-hour activity timeouts
- Retry policy with maximum 3 attempts
- Parallel execution of independent activities
- Conditional branching based on activity results