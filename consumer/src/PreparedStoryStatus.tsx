import type { PreparedStoryDelivery } from "./types";

export function PreparedStoryStatus({ delivery }: { delivery?: PreparedStoryDelivery }) {
  if (!delivery) return null;
  const prepared = new Date(delivery.preparedAtUtc);
  if (!Number.isFinite(prepared.getTime())) return null;
  const date = new Intl.DateTimeFormat(undefined, {
    year: "numeric", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short",
  }).format(prepared);
  const update = delivery.isLatest ? null
    : delivery.latestStatus === "preparing" ? "Showing the last prepared story. An update is being prepared."
      : delivery.latestStatus === "failed" ? "Showing the last prepared story. The latest update is unavailable."
        : "Showing the last prepared story.";
  return <div className="story-prepared-status" role="status">
    <span>Prepared on <time dateTime={delivery.preparedAtUtc}>{date}</time></span>
    {update && <span>{update}</span>}
  </div>;
}
