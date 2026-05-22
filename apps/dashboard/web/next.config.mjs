/** @type {import('next').NextConfig} */
const nextConfig = {
  // Static export: `next build` emite HTML/CSS/JS estáticos em `out/`.
  // O FastAPI (pulso-serve) serve esse diretório no mesmo container/origem,
  // exatamente como fazia com o `dist/` do Vite. Sem servidor Node em prod.
  output: "export",
  reactStrictMode: true,
  // next/image não funciona com `output: export` sem loader; não usamos.
  images: { unoptimized: true },
};

export default nextConfig;
