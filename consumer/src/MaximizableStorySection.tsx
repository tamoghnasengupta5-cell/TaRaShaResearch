import { useEffect, useId, useState } from "react";
import type { ReactNode } from "react";

export function MaximizableStorySection({
  className,
  label,
  children,
  chapterTitle = "Company story",
  description,
  focusControls,
  focusMeta,
  focusSource,
}: {
  className: string;
  label: string;
  children: ReactNode;
  chapterTitle?: string;
  description?: string;
  focusControls?: ReactNode;
  focusMeta?: ReactNode;
  focusSource?: ReactNode;
}) {
  const [maximized, setMaximized] = useState(false);
  const sectionId = useId();

  useEffect(() => {
    if (!maximized) return undefined;
    const previousOverflow = document.body.style.overflow;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMaximized(false);
    };
    document.body.style.overflow = "hidden";
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [maximized]);

  return <>
    {maximized && <div
      className="story-section-backdrop"
      aria-hidden="true"
      onMouseDown={() => setMaximized(false)}
    />}
    <article
      id={sectionId}
      className={`${className} maximizable-story-section ${maximized ? "is-maximized" : ""}`}
      role={maximized ? "dialog" : undefined}
      aria-modal={maximized ? true : undefined}
      aria-label={maximized ? label : undefined}
    >
      {!maximized && <button
        type="button"
        className="story-section-size-button"
        aria-label={`Maximize ${label}`}
        aria-pressed="false"
        title={`Focus on ${label}`}
        onClick={() => setMaximized(true)}
      >
        <svg viewBox="0 0 20 20" aria-hidden="true">
          <path d="M8 3H3v5M12 17h5v-5M3 3l5 5M17 17l-5-5" />
        </svg>
      </button>}
      {maximized && <>
        <header className="story-focus-header">
          <div>
            <p><span>{chapterTitle}</span><i aria-hidden="true">/</i>{label}</p>
            <h2>{label}</h2>
            {description && <small>{description}</small>}
          </div>
          <button type="button" onClick={() => setMaximized(false)} aria-label={`Return to ${chapterTitle}`}>
            <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M8 3v5H3M12 17v-5h5M3 8l5-5M17 12l-5 5" /></svg>
            <span>Return to chapter</span>
          </button>
        </header>
        <div className="story-focus-toolbar">
          <div>{focusControls}</div>
          <span className="story-focus-badge"><i aria-hidden="true" />Focused view</span>
          {focusMeta && <span className="story-focus-meta">{focusMeta}</span>}
        </div>
      </>}
      {children}
      {maximized && <footer className="story-focus-footer">
        <span><i aria-hidden="true">↖</i> Hover over the chart to inspect exact values</span>
        {focusSource && <span>{focusSource}</span>}
      </footer>}
    </article>
  </>;
}
