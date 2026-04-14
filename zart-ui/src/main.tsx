import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import { Dashboard } from "./components/Dashboard";
import { ExecutionList } from "./components/ExecutionList";
import { ExecutionDetail } from "./components/ExecutionDetail";
import { PauseRules } from "./components/PauseRules";
import "./index.css";

function Layout() {
  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <h1>zart</h1>
          <span>admin</span>
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
