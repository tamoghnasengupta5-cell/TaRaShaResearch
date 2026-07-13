import { defineConfig } from "vitest/config";
import { loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  const devApiTarget = env.DEV_API_TARGET?.replace(/\/$/, "");

  return {
    plugins: [react()],
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
