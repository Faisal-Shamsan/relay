import type { NextConfig } from "next";

const server = process.env.RELAY_SERVER_URL ?? "http://127.0.0.1:8080";

const nextConfig: NextConfig = {
  async rewrites() {
    return [{ source: "/relay/:path*", destination: `${server}/:path*` }];
  },
};

export default nextConfig;
