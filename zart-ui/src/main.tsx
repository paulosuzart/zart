import { StrictMode, useState, useEffect } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import { Dashboard } from "./components/Dashboard";
import { ExecutionList } from "./components/ExecutionList";
import { ExecutionDetail } from "./components/ExecutionDetail";
import { PauseRules } from "./components/PauseRules";
import { getCurrentBaseUrl, setBaseUrl } from "./api/client";
import "./index.css";

function ServerInput() {
  const [url, setUrl] = useState(getCurrentBaseUrl());
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    setUrl(getCurrentBaseUrl());
  }, []);

  function handleSave() {
    const trimmed = url.replace(/\/+$/, "");
    setBaseUrl(trimmed);
    setUrl(trimmed);
    setSaved(true);
    setTimeout(() => setSaved(false), 1500);
  }

  return (
    <div className="sidebar-server">
      <label>API Server</label>
      <div className="sidebar-server-row">
        <input
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") handleSave();
          }}
          placeholder="http://localhost:3000"
        />
        <button className="btn btn-sm" onClick={handleSave}>
          {saved ? "Saved" : "Set"}
        </button>
      </div>
    </div>
  );
}

function Layout() {
  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <img src="/logo.svg" alt="Zart" />
          <div className="sidebar-brand-text">
            <h1>zart</h1>
            <span>admin</span>
          </div>
        </div>
        <nav>
          <NavLink to="/" end className={({ isActive }) => (isActive ? "active" : "")}>
            Dashboard
          </NavLink>
          <NavLink to="/executions" className={({ isActive }) => (isActive ? "active" : "")}>
            Executions
          </NavLink>
          <NavLink to="/pause-rules" className={({ isActive }) => (isActive ? "active" : "")}>
            Pause Rules
          </NavLink>
        </nav>
        <ServerInput />
      </aside>
      <main className="main">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/executions" element={<ExecutionList />} />
          <Route path="/executions/:id" element={<ExecutionDetail />} />
          <Route path="/pause-rules" element={<PauseRules />} />
        </Routes>
      </main>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BrowserRouter>
      <Layout />
    </BrowserRouter>
  </StrictMode>,
);
