// frontend/src/app/tools/toolRegistry.js
import ArbWindow from "../../features/arb/ArbWindow";
import TopGainersWindow from "../../features/scanners/TopGainersWindow";
import MarketCapWindow from "../../features/scanners/MarketCapWindow";
import VolumeWindow from "../../features/scanners/VolumeWindow";
import LosersWindow from "../../features/scanners/LosersWindow";

export const TOOL_DEFS = [
  {
    id: "arb",
    title: "Arb",
    width: 920,
    height: 560,
    defaultOpen: false,
    payload: { pollEnabled: true, pollSeconds: 300 },
    Component: ArbWindow,
  },
  {
    id: "top_gainers",
    title: "Top Gainers",
    width: 980,
    height: 620,
    defaultOpen: false,
    payload: { pollEnabled: true, pollSeconds: 300 },
    Component: TopGainersWindow,
  },
  {
    id: "market_cap",
    title: "Market Cap",
    width: 980,
    height: 620,
    defaultOpen: false,
    payload: { pollEnabled: true, pollSeconds: 300 },
    Component: MarketCapWindow,
  },
  {
    id: "volume",
    title: "Volume",
    width: 980,
    height: 620,
    defaultOpen: false,
    payload: { pollEnabled: true, pollSeconds: 300 },
    Component: VolumeWindow,
  },
  {
    id: "losers",
    title: "Losers",
    width: 980,
    height: 620,
    defaultOpen: false,
    payload: { pollEnabled: true, pollSeconds: 300 },
    Component: LosersWindow,
  },
];
