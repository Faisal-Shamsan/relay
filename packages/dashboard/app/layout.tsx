import type { ReactNode } from "react";
import "./globals.css";

export const metadata = { title: "Relay" };

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <header>
          <a href="/">Relay</a>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
