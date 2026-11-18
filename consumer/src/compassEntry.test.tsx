import { afterEach, describe, expect, it, vi } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { Header, Home } from "./App";

const headerProps = {
  navigate: vi.fn(),
  user: null,
  logout: vi.fn(),
  onSelectCompany: vi.fn(async () => {}),
};

afterEach(() => vi.unstubAllEnvs());

describe("independent Compass entry", () => {
  it("places Compass before Discover and links directly to the local Compass login", () => {
    vi.stubEnv("VITE_COMPASS_URL", "");
    const html = renderToStaticMarkup(<Home navigate={vi.fn()} />);
    expect(html).toContain('href="http://localhost:5174/#/compass/login"');
    expect(html.indexOf("Login to Compass")).toBeLessThan(html.indexOf("Log in to Discover"));
    expect(html).toMatch(/<button[^>]*>Log in to Discover/);
    expect(html).toContain("research-journey-hero-mockup.jpg");
  });

  it("uses the configured Compass destination without an account or session parameter", () => {
    vi.stubEnv("VITE_COMPASS_URL", "http://localhost:6174/#/compass/login");
    const html = renderToStaticMarkup(<Home navigate={vi.fn()} />);
    expect(html).toContain('href="http://localhost:6174/#/compass/login"');
    expect(html).not.toContain("http://localhost:5174/");
    expect(html).not.toMatch(/[?&](?:token|username|session)=/);
  });

  it("uses plain TaRaSha only on the unauthenticated initial landing", () => {
    const initial = renderToStaticMarkup(<Header {...headerProps} page="home" />);
    expect(initial).toContain('alt="TaRaSha"');
    expect(initial).toContain("tarasha-logo.svg");
    expect(initial).not.toContain('alt="TaRaSha Discover"');
    const login = renderToStaticMarkup(<Header {...headerProps} page="login" />);
    expect(login).toContain('alt="TaRaSha Discover"');
    expect(login).not.toContain("brand-shared");
    const authenticated = renderToStaticMarkup(<Header {...headerProps} page="home" user={{ name: "Business owner", username: "owner", role: "user" }} />);
    expect(authenticated).toContain('alt="TaRaSha Discover"');
    expect(authenticated).not.toContain("brand-shared");
  });
});
