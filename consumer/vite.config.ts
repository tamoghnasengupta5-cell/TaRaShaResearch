import { defineConfig } from "vitest/config";
import { loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { localResearchApiPlugin } from "./localResearchDev";
import { localAuthApiPlugin } from "./localAuthDev";

export default defineConfig(({ command, mode }) => {
  const env = loadEnv(mode, ".", "");
  const localAuthApi = command === "serve" ? localAuthApiPlugin() : null;
  const localResearchApi = command === "serve" ? localResearchApiPlugin() : null;
  const devApiTarget = localResearchApi ? "" : env.DEV_API_TARGET?.replace(/\/$/, "");

  return {
    plugins: [react(), ...(localAuthApi ? [localAuthApi] : []), ...(localResearchApi ? [localResearchApi] : [])],
    base: "./",
    server: devApiTarget
      ? {
          proxy: {
            "/api": {
              target: devApiTarget,
              changeOrigin: true,
              secure: true,
            },
          },
        }
      : undefined,
    test: {
      environment: "node",
    },
  };
});
