import type { Config } from "tailwindcss";

// Paleta "trading-desk" escura. Cores com significado (princípio data-ink da
// wiki/conceitos/dashboard-design): verde = alta, vermelho = baixa, âmbar/rosa
// = severidade de anomalia. Nada decorativo.
const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}", "./lib/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: {
          DEFAULT: "#0a0c10", // fundo quase-preto
          soft: "#0f1318", // cartões
          inset: "#161b22", // inputs / linhas
        },
        line: "#1f2630", // bordas
        ink: {
          DEFAULT: "#e6edf3", // texto principal
          dim: "#9aa7b4", // texto secundário
          faint: "#5b6675", // texto terciário
        },
        up: { DEFAULT: "#26d196", soft: "#0e2f25" }, // alta
        down: { DEFAULT: "#ff5d6c", soft: "#33161b" }, // baixa
        accent: { DEFAULT: "#5b8cff", soft: "#16203a" }, // marca / seleção
        warn: { DEFAULT: "#f5a623", soft: "#332615" }, // anomalia moderada
        crit: { DEFAULT: "#ff4d6d", soft: "#33161e" }, // anomalia severa
      },
      fontFamily: {
        mono: ["var(--font-mono)", "ui-monospace", "SFMono-Regular", "monospace"],
        sans: ["var(--font-sans)", "system-ui", "sans-serif"],
      },
      boxShadow: {
        card: "0 1px 0 rgba(255,255,255,0.02), 0 8px 24px rgba(0,0,0,0.35)",
        glow: "0 0 0 1px rgba(91,140,255,0.35), 0 0 24px rgba(91,140,255,0.18)",
      },
      keyframes: {
        pulseDot: {
          "0%, 100%": { opacity: "1", transform: "scale(1)" },
          "50%": { opacity: "0.35", transform: "scale(0.85)" },
        },
        flash: {
          "0%": { backgroundColor: "rgba(91,140,255,0.18)" },
          "100%": { backgroundColor: "transparent" },
        },
      },
      animation: {
        pulseDot: "pulseDot 1.6s ease-in-out infinite",
        flash: "flash 0.6s ease-out",
      },
    },
  },
  plugins: [],
};

export default config;
