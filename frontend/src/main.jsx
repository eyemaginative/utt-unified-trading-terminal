import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { UnifiedWalletProvider } from "@jup-ag/wallet-adapter";
import "./index.css";
import App from "./App.jsx";

const unifiedWalletConfig = {
  autoConnect: false,
  env: "mainnet-beta",
  metadata: {
    name: "Unified Trading Terminal",
    description: "Unified Trading Terminal",
    url: typeof window !== "undefined" && window.location?.origin ? window.location.origin : "http://localhost:5173",
    iconUrls: ["https://jup.ag/favicon.ico"],
  },
  walletlistExplanation: {
    href: "https://dev.jup.ag/tool-kits/wallet-kit",
  },
  theme: "dark",
  lang: "en",
};

createRoot(document.getElementById("root")).render(
  <StrictMode>
    <UnifiedWalletProvider wallets={[]} config={unifiedWalletConfig}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </UnifiedWalletProvider>
  </StrictMode>
);
