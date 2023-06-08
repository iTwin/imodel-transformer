process.stdin.setEncoding("utf8");
const chunks = [];
process.stdin.on("data", (data) => chunks.push(data));
process.stdin.on("end", () => {
  const rawData = chunks.join("\n");
  const data = rawData
    .split("\n")
    .filter(Boolean)
    .map((j) => JSON.parse(j))
    .map((j) => ({
      ...j,
      time: j["Entity processing time"],
      size: parseFloat(j["Size (Gb)"]),
    }))
    .filter((j) => j.time !== "N/A")
    .sort((a, b) => b.size - a.size)
    .map((j, i, a) => ({
      ...j,
      "Size Factor": j.size / (a[i + 1]?.size ?? 0),
      "Time Factor": j.time / (a[i + 1]?.time ?? 0),
    }));
  if (process.argv.includes("--json")) {
    console.log(JSON.stringify(data));
    return;
  }
  const table = data.map(({ Id, time, size, ...o }) => ({
    ...o,
    Name: o.Name.slice(0, 40),
  }));
  console.table(table);
});
