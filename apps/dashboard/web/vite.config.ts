import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Proxy /api e /ws para a API FastAPI em :8000 (dev apenas).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://localhost:8000",
      "/ws": {
        target: "ws://localhost:8000",
        ws: true,
      },
    },
  },
  test: {
    environment: "node",
  },
});
