import { describe, expect, test } from "bun:test";
import {
  filenameFromURL,
  looksLikeMarkdown,
  hasMarkdownExtension,
  MARKDOWN_INDICATORS,
} from "./cli.js";

describe("filenameFromURL", () => {
  test("preserves .pdf extension", () => {
    expect(filenameFromURL("https://example.com/paper.pdf")).toBe("paper.pdf");
  });

  test("preserves .md extension", () => {
    expect(filenameFromURL("https://example.com/README.md")).toBe("README.md");
  });

  test("preserves .markdown extension", () => {
    expect(filenameFromURL("https://example.com/doc.markdown")).toBe(
      "doc.markdown",
    );
  });

  test("defaults to .pdf for unknown extensions", () => {
    expect(filenameFromURL("https://example.com/document")).toBe(
      "document.pdf",
    );
  });

  test("does NOT infer .md from path containing .md (false positive fix)", () => {
    // This was the bug: pathname.includes(".md") was too broad
    // e.g., https://example.com/markdown-docs/file should NOT get .md appended
    expect(filenameFromURL("https://example.com/markdown-docs/file")).toBe(
      "file.pdf",
    );
    expect(filenameFromURL("https://example.com/docs.md.backup/file")).toBe(
      "file.pdf",
    );
  });

  test("handles query strings correctly", () => {
    expect(filenameFromURL("https://example.com/doc.pdf?token=abc")).toBe(
      "doc.pdf",
    );
  });

  test("handles GitHub raw URLs with .md extension", () => {
    expect(
      filenameFromURL(
        "https://raw.githubusercontent.com/user/repo/main/README.md",
      ),
    ).toBe("README.md");
  });
});

describe("hasMarkdownExtension", () => {
  test("returns true for .md extension", () => {
    expect(hasMarkdownExtension("https://example.com/file.md")).toBe(true);
  });

  test("returns true for .markdown extension", () => {
    expect(hasMarkdownExtension("https://example.com/file.markdown")).toBe(
      true,
    );
  });

  test("returns false for .pdf extension", () => {
    expect(hasMarkdownExtension("https://example.com/file.pdf")).toBe(false);
  });

  test("returns false for no extension", () => {
    expect(hasMarkdownExtension("https://example.com/file")).toBe(false);
  });

  test("returns false for .txt extension", () => {
    expect(hasMarkdownExtension("https://example.com/file.txt")).toBe(false);
  });

  test("is case insensitive", () => {
    expect(hasMarkdownExtension("https://example.com/file.MD")).toBe(true);
    expect(hasMarkdownExtension("https://example.com/file.MARKDOWN")).toBe(
      true,
    );
  });

  test("does NOT match .md in path (only extension)", () => {
    // This is the key fix - .md in the path should not trigger markdown detection
    expect(hasMarkdownExtension("https://example.com/markdown-docs/file")).toBe(
      false,
    );
    expect(
      hasMarkdownExtension("https://example.com/docs.md.backup/file.txt"),
    ).toBe(false);
  });
});

describe("looksLikeMarkdown", () => {
  test("detects h1 heading", () => {
    expect(looksLikeMarkdown("# Hello World")).toBe(true);
  });

  test("detects h2 heading", () => {
    expect(looksLikeMarkdown("## Section")).toBe(true);
  });

  test("detects h3-h6 headings", () => {
    expect(looksLikeMarkdown("### Subsection")).toBe(true);
    expect(looksLikeMarkdown("#### Deep")).toBe(true);
    expect(looksLikeMarkdown("###### Deepest")).toBe(true);
  });

  test("detects unordered list with dash", () => {
    expect(looksLikeMarkdown("- item one\n- item two")).toBe(true);
  });

  test("detects unordered list with asterisk", () => {
    expect(looksLikeMarkdown("* item one\n* item two")).toBe(true);
  });

  test("detects unordered list with plus", () => {
    expect(looksLikeMarkdown("+ item one\n+ item two")).toBe(true);
  });

  test("detects ordered list", () => {
    expect(looksLikeMarkdown("1. First\n2. Second")).toBe(true);
  });

  test("detects code fence", () => {
    expect(looksLikeMarkdown("```javascript\nconst x = 1;\n```")).toBe(true);
  });

  test("detects table", () => {
    expect(looksLikeMarkdown("| Col1 | Col2 |\n|------|------|")).toBe(true);
  });

  test("detects markdown link", () => {
    expect(
      looksLikeMarkdown("Check out [this link](https://example.com)"),
    ).toBe(true);
  });

  test("returns false for plain text", () => {
    expect(
      looksLikeMarkdown("This is just plain text without any markers."),
    ).toBe(false);
  });

  test("returns false for text with hash not at line start", () => {
    expect(looksLikeMarkdown("This has a # in the middle")).toBe(false);
  });

  test("returns false for text with dash not at line start", () => {
    expect(looksLikeMarkdown("This has a - in the middle")).toBe(false);
  });

  test("detects markdown in multiline content", () => {
    const content = `Some intro text

## Section Header

This is a paragraph.

- List item 1
- List item 2
`;
    expect(looksLikeMarkdown(content)).toBe(true);
  });

  test("returns false for empty content", () => {
    expect(looksLikeMarkdown("")).toBe(false);
  });

  test("returns false for whitespace only", () => {
    expect(looksLikeMarkdown("   \n\n   ")).toBe(false);
  });
});

describe("Markdown MIME type detection (conceptual)", () => {
  // These tests document the expected behavior of the downloadFile function
  // They test the logic conceptually since downloadFile requires network access

  const isExplicitMarkdownMime = (contentType: string) =>
    contentType.includes("text/markdown") ||
    contentType.includes("text/x-markdown");

  test("text/markdown is explicit markdown MIME", () => {
    expect(isExplicitMarkdownMime("text/markdown")).toBe(true);
    expect(isExplicitMarkdownMime("text/markdown; charset=utf-8")).toBe(true);
  });

  test("text/x-markdown is explicit markdown MIME", () => {
    expect(isExplicitMarkdownMime("text/x-markdown")).toBe(true);
  });

  test("text/plain is NOT explicit markdown MIME", () => {
    expect(isExplicitMarkdownMime("text/plain")).toBe(false);
    expect(isExplicitMarkdownMime("text/plain; charset=utf-8")).toBe(false);
  });

  test("text/html is NOT explicit markdown MIME", () => {
    expect(isExplicitMarkdownMime("text/html")).toBe(false);
  });
});
