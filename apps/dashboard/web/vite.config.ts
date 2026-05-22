import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// Proxy /api e /ws para a API FastAPI.
// Defina VITE_API_URL em .env.local para apontar ao backend de prod:
//   VITE_API_URL=https://pulso-serve-ohnrcotlsq-uc.a.run.app
// Por padrão usa localhost:8000 (make serve).
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "VITE_");
  const apiBase = env.VITE_API_URL ?? "http://localhost:8000";
  const wsBase = apiBase.replace(/^https/, "wss").replace(/^http/, "ws");

  return {
    plugins: [react()],
    server: {
      proxy: {
        "/api": { target: apiBase, changeOrigin: true },
        "/ws": { target: wsBase, ws: true, changeOrigin: true },
      },
    },
    test: {
      environment: "node",
    },
  };
});
