interface Props {
  data: number[];
  width?: number;
  height?: number;
  className?: string;
}

// Sparkline minimalista (linha pura, sem eixos nem decoração — alto data-ink).
// Cor segue a direção: verde se sobe no período, vermelho se cai.
export function Sparkline({ data, width = 120, height = 36, className }: Props) {
  if (data.length < 2) {
    return <div style={{ width, height }} className={className} />;
  }
  const min = Math.min(...data);
  const max = Math.max(...data);
  const span = max - min || 1;
  const stepX = width / (data.length - 1);
  const pad = 2;
  const usable = height - pad * 2;

  const points = data
    .map((v, i) => {
      const x = i * stepX;
      const y = pad + usable - ((v - min) / span) * usable;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");

  const up = data[data.length - 1] >= data[0];
  const stroke = up ? "#26d196" : "#ff5d6c";
  const fill = up ? "rgba(38,209,150,0.10)" : "rgba(255,93,108,0.10)";
  const areaPoints = `0,${height} ${points} ${width},${height}`;

  return (
    <svg width={width} height={height} className={className} preserveAspectRatio="none">
      <polygon points={areaPoints} fill={fill} stroke="none" />
      <polyline
        points={points}
        fill="none"
        stroke={stroke}
        strokeWidth={1.5}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    </svg>
  );
}
