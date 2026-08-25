import { defineConfig } from "vitest/config";
import { loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { localDataApiPlugin } from "./localDataDev";
import { localAuthApiPlugin } from "./localAuthDev";

export default defineConfig(({ command, mode }) => {
  const env = loadEnv(mode, ".", "");
  const localAuthApi = command === "serve" ? localAuthApiPlugin() : null;
  const configuredProxy = env.DEV_API_TARGET?.replace(/\/$/, "");
  const localDataApi = command === "serve" && !configuredProxy
    ? localDataApiPlugin(env.TARASHA_DATA_API_URL || "http://127.0.0.1:8000", env.TARASHA_DATA_API_KEY)
    : null;
  const devApiTarget = localDataApi ? "" : configuredProxy;

  return {
    plugins: [react(), ...(localAuthApi ? [localAuthApi] : []), ...(localDataApi ? [localDataApi] : [])],
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
