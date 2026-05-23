import { useState, useEffect, useRef, useCallback, useMemo } from "react";

// ═══════════════════════════════════════════════════════════════
// OPTIML UNIVERSAL ENGINE
// AI/ML/OR Technique Auto-Applicator Across Industries
// ═══════════════════════════════════════════════════════════════

// ── Technique Knowledge Base ──────────────────────────────────
const TECHNIQUES = {
  optimization: [
    { id: "lp", name: "Linear Programming", icon: "📐", complexity: "Low", desc: "Optimize linear objective under linear constraints", useCases: ["Resource allocation", "Production planning", "Diet optimization", "Blending problems"], industries: ["manufacturing", "finance", "agriculture", "energy", "logistics"] },
    { id: "ip", name: "Integer/Mixed-Integer Programming", icon: "🔢", complexity: "Medium", desc: "LP with integer decision variables", useCases: ["Facility location", "Crew scheduling", "Capital budgeting", "Bin packing"], industries: ["logistics", "finance", "manufacturing", "retail", "telecom"] },
    { id: "qp", name: "Quadratic Programming", icon: "📈", complexity: "Medium", desc: "Quadratic objective with linear constraints", useCases: ["Portfolio optimization", "Model predictive control", "SVM training"], industries: ["finance", "manufacturing", "energy"] },
    { id: "convex", name: "Convex Optimization", icon: "⬡", complexity: "Medium", desc: "Global optimum guaranteed for convex problems", useCases: ["Signal processing", "Statistical estimation", "Network design"], industries: ["telecom", "finance", "healthcare"] },
    { id: "ga", name: "Genetic Algorithm", icon: "🧬", complexity: "Medium", desc: "Evolution-inspired metaheuristic search", useCases: ["Feature selection", "Neural architecture search", "Scheduling"], industries: ["manufacturing", "logistics", "telecom", "healthcare"] },
    { id: "sa", name: "Simulated Annealing", icon: "🌡️", complexity: "Medium", desc: "Probabilistic technique for global optimization", useCases: ["VLSI design", "Traveling salesman", "Job-shop scheduling"], industries: ["manufacturing", "logistics", "telecom"] },
    { id: "pso", name: "Particle Swarm Optimization", icon: "🐝", complexity: "Medium", desc: "Swarm intelligence optimization", useCases: ["Neural network training", "Power system dispatch", "Antenna design"], industries: ["energy", "telecom", "manufacturing"] },
    { id: "bayesopt", name: "Bayesian Optimization", icon: "🎯", complexity: "High", desc: "Sequential model-based optimization", useCases: ["Hyperparameter tuning", "Drug discovery", "A/B testing", "Materials science"], industries: ["healthcare", "retail", "manufacturing", "finance"] },
    { id: "gd", name: "Gradient Descent Variants", icon: "⬇️", complexity: "Low", desc: "SGD, Adam, RMSProp for differentiable objectives", useCases: ["Neural network training", "Logistic regression", "Matrix factorization"], industries: ["all"] },
    { id: "de", name: "Differential Evolution", icon: "🔄", complexity: "Medium", desc: "Population-based stochastic optimizer", useCases: ["Chemical engineering", "Filter design", "Calibration"], industries: ["manufacturing", "energy", "healthcare"] },
  ],
  ml: [
    { id: "linreg", name: "Linear/Ridge/Lasso Regression", icon: "📊", complexity: "Low", desc: "Predict continuous target with regularization", useCases: ["Price prediction", "Demand forecasting", "Risk scoring"], industries: ["finance", "retail", "real_estate", "energy"] },
    { id: "logreg", name: "Logistic Regression", icon: "🔀", complexity: "Low", desc: "Binary/multi-class classification baseline", useCases: ["Churn prediction", "Credit scoring", "Disease diagnosis"], industries: ["finance", "healthcare", "retail", "telecom"] },
    { id: "rf", name: "Random Forest / GBM / XGBoost", icon: "🌲", complexity: "Medium", desc: "Ensemble tree methods for tabular data", useCases: ["Fraud detection", "Customer segmentation", "Predictive maintenance"], industries: ["finance", "manufacturing", "retail", "healthcare", "telecom"] },
    { id: "svm", name: "Support Vector Machines", icon: "⚔️", complexity: "Medium", desc: "Maximum margin classifier", useCases: ["Text classification", "Image recognition", "Bioinformatics"], industries: ["healthcare", "manufacturing", "telecom"] },
    { id: "kmeans", name: "K-Means / DBSCAN Clustering", icon: "🎯", complexity: "Low", desc: "Unsupervised grouping of similar data", useCases: ["Customer segmentation", "Anomaly detection", "Document clustering"], industries: ["retail", "finance", "healthcare", "telecom"] },
    { id: "pca", name: "PCA / Dimensionality Reduction", icon: "🔍", complexity: "Low", desc: "Reduce feature space preserving variance", useCases: ["Feature engineering", "Visualization", "Noise reduction"], industries: ["all"] },
    { id: "ts", name: "ARIMA / Prophet / Exponential Smoothing", icon: "📅", complexity: "Medium", desc: "Classical time series forecasting", useCases: ["Sales forecasting", "Inventory planning", "Capacity planning"], industries: ["retail", "manufacturing", "energy", "finance", "logistics"] },
    { id: "lstm", name: "LSTM / GRU / Temporal CNNs", icon: "🧠", complexity: "High", desc: "Deep learning for sequential data", useCases: ["Stock prediction", "NLP", "Anomaly detection", "Speech"], industries: ["finance", "healthcare", "manufacturing", "telecom"] },
    { id: "nn", name: "Neural Networks (MLP/CNN)", icon: "🕸️", complexity: "High", desc: "Deep learning for complex patterns", useCases: ["Image classification", "Recommendation systems", "NLP"], industries: ["healthcare", "retail", "manufacturing", "telecom"] },
    { id: "rl", name: "Reinforcement Learning", icon: "🎮", complexity: "High", desc: "Agent learns optimal policy via rewards", useCases: ["Dynamic pricing", "Robot control", "Game AI", "Trading"], industries: ["finance", "manufacturing", "logistics", "energy"] },
    { id: "survival", name: "Survival Analysis", icon: "⏳", complexity: "Medium", desc: "Time-to-event modeling with censoring", useCases: ["Customer lifetime", "Equipment failure", "Clinical trials"], industries: ["healthcare", "manufacturing", "finance", "telecom"] },
    { id: "anomaly", name: "Anomaly Detection (Isolation Forest/Autoencoders)", icon: "🚨", complexity: "Medium", desc: "Detect outliers and unusual patterns", useCases: ["Fraud detection", "Network intrusion", "Quality control"], industries: ["finance", "manufacturing", "telecom", "healthcare"] },
  ],
  operations_research: [
    { id: "vrp", name: "Vehicle Routing Problem (VRP)", icon: "🚛", complexity: "High", desc: "Optimize delivery routes for fleet", useCases: ["Last-mile delivery", "Waste collection", "Field service"], industries: ["logistics", "retail", "energy"] },
    { id: "tsp", name: "Traveling Salesman Problem", icon: "🗺️", complexity: "High", desc: "Shortest route visiting all nodes", useCases: ["Sales territory", "PCB drilling", "Genome sequencing"], industries: ["logistics", "manufacturing", "healthcare"] },
    { id: "jobshop", name: "Job-Shop / Flow-Shop Scheduling", icon: "🏭", complexity: "High", desc: "Optimal sequencing of jobs on machines", useCases: ["Production scheduling", "OR scheduling", "Batch processing"], industries: ["manufacturing", "healthcare", "logistics"] },
    { id: "inventory", name: "Inventory Optimization (EOQ/Newsvendor)", icon: "📦", complexity: "Medium", desc: "Minimize holding + shortage costs", useCases: ["Reorder point", "Safety stock", "Multi-echelon"], industries: ["retail", "manufacturing", "logistics", "healthcare"] },
    { id: "queue", name: "Queueing Theory (M/M/c, M/G/1)", icon: "🚶", complexity: "Medium", desc: "Model waiting lines and service systems", useCases: ["Call center staffing", "Hospital capacity", "Network buffers"], industries: ["telecom", "healthcare", "retail", "logistics"] },
    { id: "network", name: "Network Flow / Min-Cost Flow", icon: "🔗", complexity: "Medium", desc: "Optimize flow through network graphs", useCases: ["Supply chain", "Telecom routing", "Pipeline design"], industries: ["logistics", "telecom", "energy", "manufacturing"] },
    { id: "assign", name: "Assignment Problem (Hungarian)", icon: "👤", complexity: "Low", desc: "Optimal one-to-one matching", useCases: ["Task assignment", "Shift scheduling", "Organ matching"], industries: ["healthcare", "manufacturing", "logistics", "retail"] },
    { id: "knapsack", name: "Knapsack / Bin Packing", icon: "🎒", complexity: "Medium", desc: "Maximize value within capacity constraints", useCases: ["Container loading", "Budget allocation", "Cutting stock"], industries: ["logistics", "finance", "manufacturing", "retail"] },
    { id: "mcs", name: "Monte Carlo Simulation", icon: "🎲", complexity: "Medium", desc: "Probabilistic modeling via random sampling", useCases: ["Risk analysis", "Option pricing", "Reliability engineering"], industries: ["finance", "manufacturing", "energy", "healthcare"] },
    { id: "dp", name: "Dynamic Programming", icon: "🧩", complexity: "High", desc: "Optimal substructure + overlapping subproblems", useCases: ["Shortest path", "Resource allocation", "Sequence alignment"], industries: ["finance", "logistics", "healthcare", "telecom"] },
    { id: "mdp", name: "Markov Decision Processes", icon: "🔁", complexity: "High", desc: "Sequential decision under uncertainty", useCases: ["Maintenance scheduling", "Inventory control", "Clinical decisions"], industries: ["healthcare", "manufacturing", "finance", "energy"] },
    { id: "game", name: "Game Theory (Nash/Stackelberg)", icon: "♟️", complexity: "High", desc: "Strategic interaction modeling", useCases: ["Pricing strategy", "Auction design", "Spectrum allocation"], industries: ["finance", "telecom", "energy", "retail"] },
  ],
};

const INDUSTRIES = [
  { id: "finance", name: "Banking & Finance", icon: "🏦", color: "#C8A96E", examples: "Credit scoring, portfolio optimization, fraud detection, risk modeling" },
  { id: "healthcare", name: "Healthcare & Pharma", icon: "🏥", color: "#4ECDC4", examples: "Clinical trials, drug discovery, patient scheduling, diagnosis" },
  { id: "manufacturing", name: "Manufacturing", icon: "🏭", color: "#FF6B6B", examples: "Quality control, predictive maintenance, production scheduling" },
  { id: "logistics", name: "Supply Chain & Logistics", icon: "🚛", color: "#45B7D1", examples: "Route optimization, inventory management, demand forecasting" },
  { id: "retail", name: "Retail & E-Commerce", icon: "🛒", color: "#96CEB4", examples: "Recommendation engines, demand forecasting, pricing optimization" },
  { id: "energy", name: "Energy & Utilities", icon: "⚡", color: "#FFEAA7", examples: "Grid optimization, load forecasting, renewable scheduling" },
  { id: "telecom", name: "Telecommunications", icon: "📡", color: "#DDA0DD", examples: "Network optimization, churn prediction, capacity planning" },
  { id: "agriculture", name: "Agriculture", icon: "🌾", color: "#8BC34A", examples: "Crop yield prediction, irrigation optimization, supply planning" },
  { id: "real_estate", name: "Real Estate", icon: "🏗️", color: "#FF9800", examples: "Price prediction, location analysis, portfolio optimization" },
];

// ── Styles ────────────────────────────────────────────────────
const theme = {
  bg: "#0A0C10",
  surface: "#12151C",
  surfaceHover: "#1A1E28",
  card: "#161A24",
  border: "#1E2330",
  borderHover: "#2A3040",
  gold: "#C8A96E",
  goldDim: "rgba(200,169,110,0.15)",
  goldGlow: "rgba(200,169,110,0.3)",
  cyan: "#4ECDC4",
  red: "#FF6B6B",
  text: "#E8E6E3",
  textDim: "#8A8F9E",
  textMuted: "#5A5F6E",
  font: "'JetBrains Mono', 'Fira Code', monospace",
  fontDisplay: "'Space Grotesk', 'Inter', sans-serif",
};

// ── CSV Parser ────────────────────────────────────────────────
function parseCSV(text) {
  const lines = text.trim().split("\n");
  if (lines.length < 2) return { headers: [], rows: [], error: "Need at least header + 1 row" };
  
  const parseRow = (line) => {
    const result = [];
    let current = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') { inQuotes = !inQuotes; }
      else if (ch === "," && !inQuotes) { result.push(current.trim()); current = ""; }
      else { current += ch; }
    }
    result.push(current.trim());
    return result;
  };

  const headers = parseRow(lines[0]);
  const rows = lines.slice(1).filter(l => l.trim()).map(parseRow);
  return { headers, rows, error: null };
}

function profileData(headers, rows) {
  const profile = headers.map((h, i) => {
    const values = rows.map(r => r[i]).filter(v => v !== "" && v !== undefined && v !== null);
    const numericVals = values.map(Number).filter(n => !isNaN(n));
    const isNumeric = numericVals.length > values.length * 0.7;
    const uniqueCount = new Set(values).size;
    const nullCount = rows.length - values.length;
    
    let stats = {};
    if (isNumeric && numericVals.length > 0) {
      const sorted = [...numericVals].sort((a, b) => a - b);
      stats = {
        min: sorted[0],
        max: sorted[sorted.length - 1],
        mean: (numericVals.reduce((a, b) => a + b, 0) / numericVals.length).toFixed(2),
        median: sorted[Math.floor(sorted.length / 2)],
        std: Math.sqrt(numericVals.reduce((sum, v) => sum + Math.pow(v - numericVals.reduce((a, b) => a + b, 0) / numericVals.length, 2), 0) / numericVals.length).toFixed(2),
      };
    }
    
    const isCategorical = !isNumeric || uniqueCount < Math.min(20, rows.length * 0.05);
    const isDate = values.some(v => !isNaN(Date.parse(v)) && isNaN(Number(v)));
    const isBinary = uniqueCount === 2;
    
    return {
      name: h,
      type: isDate ? "datetime" : isNumeric ? "numeric" : "categorical",
      isBinary,
      uniqueCount,
      nullCount,
      nullPct: ((nullCount / rows.length) * 100).toFixed(1),
      sampleValues: values.slice(0, 5),
      ...stats,
    };
  });
  
  return {
    rowCount: rows.length,
    colCount: headers.length,
    columns: profile,
    numericCols: profile.filter(c => c.type === "numeric").length,
    categoricalCols: profile.filter(c => c.type === "categorical").length,
    datetimeCols: profile.filter(c => c.type === "datetime").length,
    hasTimeSeries: profile.some(c => c.type === "datetime"),
    hasBinaryTarget: profile.some(c => c.isBinary),
  };
}

// ── Main App Component ────────────────────────────────────────
export default function OptiMLEngine() {
  const [view, setView] = useState("home"); // home, techniques, industry, engine
  const [selectedCategory, setSelectedCategory] = useState("all");
  const [selectedIndustry, setSelectedIndustry] = useState(null);
  const [uploadedData, setUploadedData] = useState(null);
  const [dataProfile, setDataProfile] = useState(null);
  const [analysisResult, setAnalysisResult] = useState(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [analyzeProgress, setAnalyzeProgress] = useState("");
  const [searchTerm, setSearchTerm] = useState("");
  const [expandedTechnique, setExpandedTechnique] = useState(null);
  const fileInputRef = useRef(null);

  // ── File Upload Handler ──
  const handleFileUpload = useCallback((e) => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      const text = ev.target.result;
      const { headers, rows, error } = parseCSV(text);
      if (error) { alert(error); return; }
      const profile = profileData(headers, rows);
      setUploadedData({ fileName: file.name, headers, rows, rawText: text });
      setDataProfile(profile);
      setAnalysisResult(null);
      setView("engine");
    };
    reader.readAsText(file);
  }, []);

  // ── AI Analysis via Claude API ──
  const runAnalysis = useCallback(async () => {
    if (!uploadedData || !dataProfile) return;
    setIsAnalyzing(true);
    setAnalyzeProgress("Profiling data structure...");
    
    const sampleRows = uploadedData.rows.slice(0, 15).map(r => uploadedData.headers.map((h, i) => `${h}: ${r[i]}`).join(", ")).join("\n");
    
    const profileSummary = dataProfile.columns.map(c => 
      `${c.name} (${c.type}${c.isBinary ? '/binary' : ''}): ${c.uniqueCount} unique, ${c.nullPct}% null${c.type === 'numeric' ? `, range [${c.min}–${c.max}], mean=${c.mean}, std=${c.std}` : `, samples: ${c.sampleValues.slice(0,3).join(', ')}`}`
    ).join("\n");

    const allTechniques = [...TECHNIQUES.optimization, ...TECHNIQUES.ml, ...TECHNIQUES.operations_research]
      .map(t => `${t.name} (${t.id}): ${t.desc} → ${t.useCases.join(", ")}`).join("\n");

    const prompt = `You are OptiML Engine — an expert AI system that analyzes datasets and recommends the best AI/ML, Optimization, and Operations Research techniques.

## DATASET PROFILE
File: ${uploadedData.fileName}
Rows: ${dataProfile.rowCount} | Columns: ${dataProfile.colCount}
Numeric: ${dataProfile.numericCols} | Categorical: ${dataProfile.categoricalCols} | DateTime: ${dataProfile.datetimeCols}
Has Time Series: ${dataProfile.hasTimeSeries} | Has Binary Target: ${dataProfile.hasBinaryTarget}

### Column Details:
${profileSummary}

### Sample Data (first 15 rows):
${sampleRows}

## AVAILABLE TECHNIQUES:
${allTechniques}

## YOUR TASK:
Analyze this dataset and return a JSON response (NO markdown, NO backticks, ONLY valid JSON) with this exact structure:

{
  "detected_industry": "string — most likely industry",
  "data_summary": "string — 2-3 sentence summary of what this data represents",
  "problem_types": ["list of detected problem types: regression, classification, clustering, time_series, optimization, scheduling, routing, etc."],
  "recommended_techniques": [
    {
      "rank": 1,
      "technique_id": "id from the list above",
      "technique_name": "name",
      "category": "optimization | ml | operations_research",
      "confidence": 0.95,
      "reasoning": "Why this technique fits this specific data",
      "target_column": "which column to predict/optimize (if applicable)",
      "feature_columns": ["which columns to use as features"],
      "expected_outcome": "What business value this would deliver",
      "implementation_steps": ["Step 1...", "Step 2...", "Step 3..."],
      "python_snippet": "Brief Python code showing how to apply (use sklearn, scipy, pulp, ortools as needed)"
    }
  ],
  "data_quality_notes": ["Any data quality issues found"],
  "preprocessing_needed": ["List of preprocessing steps recommended"],
  "advanced_pipeline": "A 3-4 sentence description of an advanced end-to-end ML/OR pipeline combining multiple techniques for maximum value"
}

Recommend 3-5 techniques ranked by fit. Include at least one from each category (ML, Optimization, OR) if applicable. Be specific about columns and real values from the data.`;

    try {
      setAnalyzeProgress("Running AI analysis engine...");
      const response = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: "claude-sonnet-4-20250514",
          max_tokens: 4000,
          messages: [{ role: "user", content: prompt }],
        }),
      });

      setAnalyzeProgress("Parsing recommendations...");
      const data = await response.json();
      const text = data.content?.map(b => b.text || "").join("") || "";
      const clean = text.replace(/```json|```/g, "").trim();
      const result = JSON.parse(clean);
      setAnalysisResult(result);
    } catch (err) {
      console.error("Analysis error:", err);
      setAnalysisResult({ error: "Analysis failed. Check console for details. Error: " + err.message });
    } finally {
      setIsAnalyzing(false);
      setAnalyzeProgress("");
    }
  }, [uploadedData, dataProfile]);

  // ── Filter Techniques ──
  const filteredTechniques = useMemo(() => {
    let techs = [];
    const cats = selectedCategory === "all" 
      ? ["optimization", "ml", "operations_research"] 
      : [selectedCategory];
    
    cats.forEach(cat => {
      TECHNIQUES[cat]?.forEach(t => {
        const matchesIndustry = !selectedIndustry || t.industries?.includes(selectedIndustry) || t.industries?.includes("all");
        const matchesSearch = !searchTerm || 
          t.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
          t.desc.toLowerCase().includes(searchTerm.toLowerCase()) ||
          t.useCases.some(u => u.toLowerCase().includes(searchTerm.toLowerCase()));
        if (matchesIndustry && matchesSearch) {
          techs.push({ ...t, category: cat });
        }
      });
    });
    return techs;
  }, [selectedCategory, selectedIndustry, searchTerm]);

  // ── Render ──
  return (
    <div style={{
      fontFamily: theme.font,
      background: theme.bg,
      color: theme.text,
      minHeight: "100vh",
      width: "100%",
      overflow: "auto",
    }}>
      {/* ── HEADER ── */}
      <header style={{
        background: `linear-gradient(180deg, ${theme.surface} 0%, ${theme.bg} 100%)`,
        borderBottom: `1px solid ${theme.border}`,
        padding: "16px 24px",
        position: "sticky",
        top: 0,
        zIndex: 100,
        backdropFilter: "blur(20px)",
      }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: "12px" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
            <div style={{
              width: 36, height: 36, borderRadius: "8px",
              background: `linear-gradient(135deg, ${theme.gold}, ${theme.cyan})`,
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: "18px", fontWeight: 800,
            }}>⚡</div>
            <div>
              <div style={{ fontSize: "16px", fontWeight: 700, color: theme.gold, letterSpacing: "2px" }}>
                OptiML ENGINE
              </div>
              <div style={{ fontSize: "10px", color: theme.textMuted, letterSpacing: "1px" }}>
                AI · ML · OPTIMIZATION · OPERATIONS RESEARCH
              </div>
            </div>
          </div>
          
          <nav style={{ display: "flex", gap: "4px" }}>
            {[
              { id: "home", label: "HOME", icon: "◆" },
              { id: "techniques", label: "TECHNIQUES", icon: "◈" },
              { id: "industry", label: "INDUSTRIES", icon: "◇" },
              { id: "engine", label: "ENGINE", icon: "▶" },
            ].map(tab => (
              <button key={tab.id} onClick={() => setView(tab.id)} style={{
                padding: "8px 16px", border: "1px solid",
                borderColor: view === tab.id ? theme.gold : theme.border,
                background: view === tab.id ? theme.goldDim : "transparent",
                color: view === tab.id ? theme.gold : theme.textDim,
                borderRadius: "6px", cursor: "pointer",
                fontSize: "11px", fontFamily: theme.font, fontWeight: 600,
                letterSpacing: "1px", transition: "all 0.2s",
              }}>
                {tab.icon} {tab.label}
              </button>
            ))}
          </nav>
        </div>
      </header>

      <main style={{ padding: "24px", maxWidth: "1400px", margin: "0 auto" }}>
        
        {/* ═══ HOME VIEW ═══ */}
        {view === "home" && (
          <div>
            <div style={{ textAlign: "center", padding: "48px 0 32px" }}>
              <h1 style={{
                fontSize: "32px", fontWeight: 800, color: theme.gold,
                letterSpacing: "3px", margin: "0 0 8px",
              }}>
                UNIVERSAL AI/ML ENGINE
              </h1>
              <p style={{ fontSize: "14px", color: theme.textDim, maxWidth: 600, margin: "0 auto 32px", lineHeight: 1.7 }}>
                Upload any dataset → Auto-detect industry & problem type → Get ranked AI/ML/OR technique recommendations with implementation code
              </p>
              
              <button onClick={() => fileInputRef.current?.click()} style={{
                padding: "16px 48px", background: `linear-gradient(135deg, ${theme.gold}, #D4B87A)`,
                border: "none", borderRadius: "8px", color: theme.bg,
                fontSize: "14px", fontWeight: 700, cursor: "pointer",
                letterSpacing: "2px", fontFamily: theme.font,
                boxShadow: `0 4px 24px ${theme.goldGlow}`,
                transition: "transform 0.2s",
              }}
              onMouseEnter={e => e.target.style.transform = "translateY(-2px)"}
              onMouseLeave={e => e.target.style.transform = "translateY(0)"}
              >
                ▶ UPLOAD DATA (.CSV)
              </button>
              <input ref={fileInputRef} type="file" accept=".csv" onChange={handleFileUpload} style={{ display: "none" }} />
              <p style={{ fontSize: "11px", color: theme.textMuted, marginTop: "12px" }}>
                Drop any CSV file — the engine handles the rest
              </p>
            </div>

            {/* Stats Grid */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: "12px", marginBottom: "32px" }}>
              {[
                { label: "OPTIMIZATION", count: TECHNIQUES.optimization.length, color: theme.gold, icon: "📐" },
                { label: "MACHINE LEARNING", count: TECHNIQUES.ml.length, color: theme.cyan, icon: "🧠" },
                { label: "OPERATIONS RESEARCH", count: TECHNIQUES.operations_research.length, color: theme.red, icon: "🔗" },
                { label: "INDUSTRIES COVERED", count: INDUSTRIES.length, color: "#DDA0DD", icon: "🏢" },
              ].map(s => (
                <div key={s.label} style={{
                  background: theme.card, border: `1px solid ${theme.border}`,
                  borderRadius: "8px", padding: "20px", textAlign: "center",
                }}>
                  <div style={{ fontSize: "28px", marginBottom: "4px" }}>{s.icon}</div>
                  <div style={{ fontSize: "28px", fontWeight: 800, color: s.color }}>{s.count}</div>
                  <div style={{ fontSize: "10px", color: theme.textMuted, letterSpacing: "1px" }}>{s.label}</div>
                </div>
              ))}
            </div>

            {/* How It Works */}
            <div style={{
              background: theme.card, border: `1px solid ${theme.border}`,
              borderRadius: "12px", padding: "24px", marginBottom: "24px",
            }}>
              <h3 style={{ color: theme.gold, fontSize: "13px", letterSpacing: "2px", margin: "0 0 20px" }}>
                ◆ HOW IT WORKS
              </h3>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: "16px" }}>
                {[
                  { step: "01", title: "UPLOAD DATA", desc: "Drop any CSV — sales, patients, inventory, transactions, sensor readings, anything." },
                  { step: "02", title: "AUTO-PROFILE", desc: "Engine detects column types, distributions, nulls, time series, binary targets, correlations." },
                  { step: "03", title: "AI ANALYSIS", desc: "Claude AI identifies industry, problem type, and ranks best-fit techniques from 30+ methods." },
                  { step: "04", title: "GET CODE", desc: "Receive Python implementation snippets, preprocessing steps, and an advanced pipeline blueprint." },
                ].map(s => (
                  <div key={s.step} style={{ display: "flex", gap: "12px" }}>
                    <div style={{
                      width: 36, height: 36, borderRadius: "50%", flexShrink: 0,
                      border: `2px solid ${theme.gold}`, display: "flex",
                      alignItems: "center", justifyContent: "center",
                      fontSize: "12px", fontWeight: 700, color: theme.gold,
                    }}>{s.step}</div>
                    <div>
                      <div style={{ fontSize: "12px", fontWeight: 700, color: theme.text, marginBottom: "4px" }}>{s.title}</div>
                      <div style={{ fontSize: "11px", color: theme.textDim, lineHeight: 1.6 }}>{s.desc}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Technique Matrix Preview */}
            <div style={{
              background: theme.card, border: `1px solid ${theme.border}`,
              borderRadius: "12px", padding: "24px",
            }}>
              <h3 style={{ color: theme.gold, fontSize: "13px", letterSpacing: "2px", margin: "0 0 16px" }}>
                ◆ TECHNIQUE × INDUSTRY COVERAGE MATRIX
              </h3>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "10px" }}>
                  <thead>
                    <tr>
                      <th style={{ padding: "8px", textAlign: "left", color: theme.textMuted, borderBottom: `1px solid ${theme.border}` }}>TECHNIQUE</th>
                      {INDUSTRIES.slice(0, 7).map(ind => (
                        <th key={ind.id} style={{ padding: "8px", textAlign: "center", color: theme.textMuted, borderBottom: `1px solid ${theme.border}`, minWidth: "60px" }}>
                          {ind.icon}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[...TECHNIQUES.optimization.slice(0, 3), ...TECHNIQUES.ml.slice(0, 3), ...TECHNIQUES.operations_research.slice(0, 3)].map(t => (
                      <tr key={t.id}>
                        <td style={{ padding: "6px 8px", color: theme.textDim, borderBottom: `1px solid ${theme.border}`, whiteSpace: "nowrap" }}>
                          {t.icon} {t.name}
                        </td>
                        {INDUSTRIES.slice(0, 7).map(ind => {
                          const match = t.industries?.includes(ind.id) || t.industries?.includes("all");
                          return (
                            <td key={ind.id} style={{ padding: "6px", textAlign: "center", borderBottom: `1px solid ${theme.border}` }}>
                              <span style={{
                                display: "inline-block", width: 12, height: 12, borderRadius: "3px",
                                background: match ? theme.gold : theme.border,
                                opacity: match ? 1 : 0.3,
                              }} />
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div style={{ fontSize: "10px", color: theme.textMuted, marginTop: "8px", textAlign: "center" }}>
                  Showing 9 of {TECHNIQUES.optimization.length + TECHNIQUES.ml.length + TECHNIQUES.operations_research.length} techniques — click TECHNIQUES tab for full catalog
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ═══ TECHNIQUES VIEW ═══ */}
        {view === "techniques" && (
          <div>
            <div style={{ display: "flex", gap: "12px", marginBottom: "20px", flexWrap: "wrap", alignItems: "center" }}>
              <input
                type="text" placeholder="Search techniques..."
                value={searchTerm} onChange={e => setSearchTerm(e.target.value)}
                style={{
                  padding: "10px 16px", background: theme.surface, border: `1px solid ${theme.border}`,
                  borderRadius: "8px", color: theme.text, fontSize: "12px",
                  fontFamily: theme.font, flex: "1", minWidth: "200px",
                  outline: "none",
                }}
              />
              {[
                { id: "all", label: "ALL" },
                { id: "optimization", label: "OPTIMIZATION" },
                { id: "ml", label: "ML" },
                { id: "operations_research", label: "OR" },
              ].map(cat => (
                <button key={cat.id} onClick={() => setSelectedCategory(cat.id)} style={{
                  padding: "8px 16px", border: "1px solid",
                  borderColor: selectedCategory === cat.id ? theme.gold : theme.border,
                  background: selectedCategory === cat.id ? theme.goldDim : "transparent",
                  color: selectedCategory === cat.id ? theme.gold : theme.textDim,
                  borderRadius: "6px", cursor: "pointer",
                  fontSize: "11px", fontFamily: theme.font, fontWeight: 600,
                }}>
                  {cat.label}
                </button>
              ))}
            </div>
            
            <div style={{ fontSize: "11px", color: theme.textMuted, marginBottom: "16px" }}>
              Showing {filteredTechniques.length} techniques
              {selectedIndustry && ` for ${INDUSTRIES.find(i => i.id === selectedIndustry)?.name}`}
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(340px, 1fr))", gap: "12px" }}>
              {filteredTechniques.map(t => {
                const isExpanded = expandedTechnique === t.id;
                const catColor = t.category === "optimization" ? theme.gold : t.category === "ml" ? theme.cyan : theme.red;
                return (
                  <div key={t.id} onClick={() => setExpandedTechnique(isExpanded ? null : t.id)}
                    style={{
                      background: theme.card, border: `1px solid ${isExpanded ? catColor : theme.border}`,
                      borderRadius: "10px", padding: "16px", cursor: "pointer",
                      transition: "all 0.2s",
                    }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "8px" }}>
                      <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                        <span style={{ fontSize: "20px" }}>{t.icon}</span>
                        <div>
                          <div style={{ fontSize: "13px", fontWeight: 700, color: theme.text }}>{t.name}</div>
                          <div style={{ fontSize: "10px", color: catColor, letterSpacing: "1px", textTransform: "uppercase" }}>
                            {t.category.replace("_", " ")}
                          </div>
                        </div>
                      </div>
                      <span style={{
                        padding: "3px 8px", borderRadius: "4px", fontSize: "9px",
                        background: t.complexity === "Low" ? "rgba(78,205,196,0.15)" : t.complexity === "Medium" ? "rgba(200,169,110,0.15)" : "rgba(255,107,107,0.15)",
                        color: t.complexity === "Low" ? theme.cyan : t.complexity === "Medium" ? theme.gold : theme.red,
                        fontWeight: 600,
                      }}>
                        {t.complexity}
                      </span>
                    </div>
                    <div style={{ fontSize: "11px", color: theme.textDim, lineHeight: 1.6, marginBottom: "8px" }}>{t.desc}</div>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: "4px" }}>
                      {t.useCases.slice(0, isExpanded ? 99 : 3).map(u => (
                        <span key={u} style={{
                          padding: "2px 8px", borderRadius: "4px", fontSize: "9px",
                          background: theme.surface, color: theme.textDim, border: `1px solid ${theme.border}`,
                        }}>{u}</span>
                      ))}
                    </div>
                    {isExpanded && (
                      <div style={{ marginTop: "12px", paddingTop: "12px", borderTop: `1px solid ${theme.border}` }}>
                        <div style={{ fontSize: "10px", color: theme.textMuted, marginBottom: "4px", letterSpacing: "1px" }}>APPLICABLE INDUSTRIES:</div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: "4px" }}>
                          {(t.industries?.includes("all") ? INDUSTRIES : INDUSTRIES.filter(i => t.industries?.includes(i.id))).map(ind => (
                            <span key={ind.id} style={{
                              padding: "3px 8px", borderRadius: "4px", fontSize: "10px",
                              background: `${ind.color}22`, color: ind.color, fontWeight: 600,
                            }}>
                              {ind.icon} {ind.name}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* ═══ INDUSTRY VIEW ═══ */}
        {view === "industry" && (
          <div>
            <h2 style={{ fontSize: "18px", color: theme.gold, letterSpacing: "2px", marginBottom: "24px" }}>
              ◇ INDUSTRY PROFILES
            </h2>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(340px, 1fr))", gap: "16px" }}>
              {INDUSTRIES.map(ind => {
                const techCount = [...TECHNIQUES.optimization, ...TECHNIQUES.ml, ...TECHNIQUES.operations_research]
                  .filter(t => t.industries?.includes(ind.id) || t.industries?.includes("all")).length;
                const isSelected = selectedIndustry === ind.id;
                return (
                  <div key={ind.id}
                    onClick={() => { setSelectedIndustry(isSelected ? null : ind.id); setView("techniques"); }}
                    style={{
                      background: theme.card, border: `1px solid ${isSelected ? ind.color : theme.border}`,
                      borderRadius: "12px", padding: "20px", cursor: "pointer",
                      transition: "all 0.2s", position: "relative", overflow: "hidden",
                    }}>
                    <div style={{
                      position: "absolute", top: 0, left: 0, right: 0, height: "3px",
                      background: `linear-gradient(90deg, ${ind.color}, transparent)`,
                    }} />
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                      <div>
                        <div style={{ fontSize: "28px", marginBottom: "8px" }}>{ind.icon}</div>
                        <div style={{ fontSize: "15px", fontWeight: 700, color: theme.text, marginBottom: "4px" }}>{ind.name}</div>
                        <div style={{ fontSize: "11px", color: theme.textDim, lineHeight: 1.6, marginBottom: "12px" }}>{ind.examples}</div>
                      </div>
                      <div style={{
                        padding: "6px 12px", borderRadius: "8px",
                        background: `${ind.color}22`, color: ind.color,
                        fontSize: "18px", fontWeight: 800,
                      }}>{techCount}</div>
                    </div>
                    <div style={{ fontSize: "10px", color: ind.color, fontWeight: 600 }}>
                      Click to view {techCount} applicable techniques →
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* ═══ ENGINE VIEW ═══ */}
        {view === "engine" && (
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "20px", flexWrap: "wrap", gap: "12px" }}>
              <h2 style={{ fontSize: "18px", color: theme.gold, letterSpacing: "2px", margin: 0 }}>
                ▶ ANALYSIS ENGINE
              </h2>
              <button onClick={() => fileInputRef.current?.click()} style={{
                padding: "8px 20px", background: theme.surface, border: `1px solid ${theme.border}`,
                borderRadius: "6px", color: theme.textDim, cursor: "pointer",
                fontSize: "11px", fontFamily: theme.font,
              }}>
                ↑ Upload New CSV
              </button>
              <input ref={fileInputRef} type="file" accept=".csv" onChange={handleFileUpload} style={{ display: "none" }} />
            </div>

            {!uploadedData ? (
              <div style={{
                background: theme.card, border: `2px dashed ${theme.border}`,
                borderRadius: "16px", padding: "64px 24px", textAlign: "center",
              }}>
                <div style={{ fontSize: "48px", marginBottom: "16px" }}>📊</div>
                <div style={{ fontSize: "16px", fontWeight: 700, color: theme.text, marginBottom: "8px" }}>
                  Drop Your Dataset Here
                </div>
                <div style={{ fontSize: "12px", color: theme.textDim, marginBottom: "24px" }}>
                  CSV format — any industry, any domain
                </div>
                <button onClick={() => fileInputRef.current?.click()} style={{
                  padding: "12px 36px", background: theme.gold, border: "none",
                  borderRadius: "8px", color: theme.bg, cursor: "pointer",
                  fontSize: "13px", fontWeight: 700, fontFamily: theme.font,
                }}>
                  SELECT FILE
                </button>
              </div>
            ) : (
              <div>
                {/* Data Profile Summary */}
                <div style={{
                  background: theme.card, border: `1px solid ${theme.border}`,
                  borderRadius: "12px", padding: "20px", marginBottom: "16px",
                }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
                    <div>
                      <div style={{ fontSize: "14px", fontWeight: 700, color: theme.text }}>
                        📄 {uploadedData.fileName}
                      </div>
                      <div style={{ fontSize: "11px", color: theme.textDim }}>
                        {dataProfile.rowCount.toLocaleString()} rows × {dataProfile.colCount} columns
                      </div>
                    </div>
                    <button onClick={runAnalysis} disabled={isAnalyzing} style={{
                      padding: "10px 28px",
                      background: isAnalyzing ? theme.surface : `linear-gradient(135deg, ${theme.gold}, #D4B87A)`,
                      border: "none", borderRadius: "8px",
                      color: isAnalyzing ? theme.textDim : theme.bg,
                      cursor: isAnalyzing ? "not-allowed" : "pointer",
                      fontSize: "12px", fontWeight: 700, fontFamily: theme.font,
                      letterSpacing: "1px",
                    }}>
                      {isAnalyzing ? `⟳ ${analyzeProgress}` : "⚡ RUN AI ANALYSIS"}
                    </button>
                  </div>

                  {/* Column Profile Table */}
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "11px" }}>
                      <thead>
                        <tr>
                          {["Column", "Type", "Unique", "Nulls", "Stats / Samples"].map(h => (
                            <th key={h} style={{
                              padding: "8px 10px", textAlign: "left", color: theme.textMuted,
                              borderBottom: `1px solid ${theme.border}`, fontSize: "10px",
                              letterSpacing: "1px",
                            }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {dataProfile.columns.map(col => (
                          <tr key={col.name}>
                            <td style={{ padding: "8px 10px", color: theme.gold, fontWeight: 600, borderBottom: `1px solid ${theme.border}` }}>
                              {col.name}
                            </td>
                            <td style={{ padding: "8px 10px", borderBottom: `1px solid ${theme.border}` }}>
                              <span style={{
                                padding: "2px 8px", borderRadius: "4px", fontSize: "9px", fontWeight: 600,
                                background: col.type === "numeric" ? "rgba(78,205,196,0.15)" : col.type === "datetime" ? "rgba(200,169,110,0.15)" : "rgba(255,107,107,0.15)",
                                color: col.type === "numeric" ? theme.cyan : col.type === "datetime" ? theme.gold : theme.red,
                              }}>
                                {col.type}{col.isBinary ? " ★" : ""}
                              </span>
                            </td>
                            <td style={{ padding: "8px 10px", color: theme.textDim, borderBottom: `1px solid ${theme.border}` }}>
                              {col.uniqueCount}
                            </td>
                            <td style={{ padding: "8px 10px", color: parseFloat(col.nullPct) > 10 ? theme.red : theme.textDim, borderBottom: `1px solid ${theme.border}` }}>
                              {col.nullPct}%
                            </td>
                            <td style={{ padding: "8px 10px", color: theme.textDim, borderBottom: `1px solid ${theme.border}`, maxWidth: "300px", overflow: "hidden", textOverflow: "ellipsis" }}>
                              {col.type === "numeric"
                                ? `range [${col.min} — ${col.max}], μ=${col.mean}, σ=${col.std}`
                                : col.sampleValues?.slice(0, 3).join(", ")}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Data Preview */}
                <div style={{
                  background: theme.card, border: `1px solid ${theme.border}`,
                  borderRadius: "12px", padding: "20px", marginBottom: "16px",
                }}>
                  <h4 style={{ fontSize: "12px", color: theme.textMuted, letterSpacing: "1px", margin: "0 0 12px" }}>
                    DATA PREVIEW (first 8 rows)
                  </h4>
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "10px" }}>
                      <thead>
                        <tr>
                          {uploadedData.headers.map(h => (
                            <th key={h} style={{
                              padding: "6px 8px", textAlign: "left", color: theme.gold,
                              borderBottom: `1px solid ${theme.border}`, whiteSpace: "nowrap",
                            }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {uploadedData.rows.slice(0, 8).map((row, i) => (
                          <tr key={i}>
                            {row.map((cell, j) => (
                              <td key={j} style={{
                                padding: "5px 8px", color: theme.textDim,
                                borderBottom: `1px solid ${theme.border}`,
                                maxWidth: "150px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                              }}>{cell}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Analysis Results */}
                {analysisResult && !analysisResult.error && (
                  <div>
                    {/* Summary Header */}
                    <div style={{
                      background: `linear-gradient(135deg, ${theme.card}, ${theme.surface})`,
                      border: `1px solid ${theme.gold}`,
                      borderRadius: "12px", padding: "24px", marginBottom: "16px",
                    }}>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: "16px" }}>
                        <div>
                          <div style={{ fontSize: "10px", color: theme.textMuted, letterSpacing: "1px", marginBottom: "4px" }}>DETECTED INDUSTRY</div>
                          <div style={{ fontSize: "20px", fontWeight: 800, color: theme.gold }}>{analysisResult.detected_industry}</div>
                        </div>
                        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                          {analysisResult.problem_types?.map(p => (
                            <span key={p} style={{
                              padding: "4px 12px", borderRadius: "6px", fontSize: "10px",
                              background: theme.goldDim, color: theme.gold, fontWeight: 600,
                              textTransform: "uppercase", letterSpacing: "1px",
                            }}>{p}</span>
                          ))}
                        </div>
                      </div>
                      <div style={{ fontSize: "12px", color: theme.textDim, marginTop: "12px", lineHeight: 1.7 }}>
                        {analysisResult.data_summary}
                      </div>
                    </div>

                    {/* Preprocessing & Quality */}
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", marginBottom: "16px" }}>
                      <div style={{
                        background: theme.card, border: `1px solid ${theme.border}`,
                        borderRadius: "10px", padding: "16px",
                      }}>
                        <h4 style={{ fontSize: "11px", color: theme.cyan, letterSpacing: "1px", margin: "0 0 10px" }}>
                          🔧 PREPROCESSING NEEDED
                        </h4>
                        {analysisResult.preprocessing_needed?.map((p, i) => (
                          <div key={i} style={{ fontSize: "11px", color: theme.textDim, padding: "4px 0", lineHeight: 1.5 }}>
                            <span style={{ color: theme.cyan }}>›</span> {p}
                          </div>
                        ))}
                      </div>
                      <div style={{
                        background: theme.card, border: `1px solid ${theme.border}`,
                        borderRadius: "10px", padding: "16px",
                      }}>
                        <h4 style={{ fontSize: "11px", color: theme.red, letterSpacing: "1px", margin: "0 0 10px" }}>
                          ⚠ DATA QUALITY NOTES
                        </h4>
                        {analysisResult.data_quality_notes?.map((n, i) => (
                          <div key={i} style={{ fontSize: "11px", color: theme.textDim, padding: "4px 0", lineHeight: 1.5 }}>
                            <span style={{ color: theme.red }}>›</span> {n}
                          </div>
                        ))}
                      </div>
                    </div>

                    {/* Recommended Techniques */}
                    <h3 style={{ fontSize: "14px", color: theme.gold, letterSpacing: "2px", margin: "24px 0 16px" }}>
                      ⚡ RECOMMENDED TECHNIQUES
                    </h3>
                    {analysisResult.recommended_techniques?.map((rec, i) => {
                      const catColor = rec.category === "optimization" ? theme.gold : rec.category === "ml" ? theme.cyan : theme.red;
                      return (
                        <div key={i} style={{
                          background: theme.card, border: `1px solid ${theme.border}`,
                          borderRadius: "12px", padding: "20px", marginBottom: "12px",
                          borderLeft: `4px solid ${catColor}`,
                        }}>
                          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "12px", flexWrap: "wrap", gap: "8px" }}>
                            <div>
                              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                                <span style={{
                                  width: 28, height: 28, borderRadius: "50%", display: "inline-flex",
                                  alignItems: "center", justifyContent: "center",
                                  background: `${catColor}22`, color: catColor,
                                  fontSize: "14px", fontWeight: 800,
                                }}>#{rec.rank}</span>
                                <span style={{ fontSize: "15px", fontWeight: 700, color: theme.text }}>{rec.technique_name}</span>
                                <span style={{
                                  padding: "2px 8px", borderRadius: "4px", fontSize: "9px",
                                  background: `${catColor}22`, color: catColor,
                                  textTransform: "uppercase", fontWeight: 600,
                                }}>{rec.category?.replace("_", " ")}</span>
                              </div>
                            </div>
                            <div style={{
                              padding: "4px 12px", borderRadius: "6px",
                              background: `${catColor}22`, color: catColor,
                              fontSize: "12px", fontWeight: 700,
                            }}>
                              {(rec.confidence * 100).toFixed(0)}% fit
                            </div>
                          </div>
                          
                          <div style={{ fontSize: "12px", color: theme.textDim, lineHeight: 1.7, marginBottom: "12px" }}>
                            {rec.reasoning}
                          </div>

                          {rec.target_column && (
                            <div style={{ fontSize: "11px", color: theme.textMuted, marginBottom: "4px" }}>
                              <strong style={{ color: theme.gold }}>Target:</strong> {rec.target_column}
                              {rec.feature_columns?.length > 0 && (
                                <span> | <strong style={{ color: theme.gold }}>Features:</strong> {rec.feature_columns.join(", ")}</span>
                              )}
                            </div>
                          )}

                          <div style={{ fontSize: "12px", color: theme.cyan, marginBottom: "12px" }}>
                            💡 {rec.expected_outcome}
                          </div>

                          <div style={{ marginBottom: "12px" }}>
                            <div style={{ fontSize: "10px", color: theme.textMuted, letterSpacing: "1px", marginBottom: "6px" }}>
                              IMPLEMENTATION STEPS
                            </div>
                            {rec.implementation_steps?.map((step, j) => (
                              <div key={j} style={{ fontSize: "11px", color: theme.textDim, padding: "3px 0", lineHeight: 1.5 }}>
                                <span style={{ color: catColor, fontWeight: 700 }}>{j + 1}.</span> {step}
                              </div>
                            ))}
                          </div>

                          {rec.python_snippet && (
                            <div style={{
                              background: theme.bg, borderRadius: "8px", padding: "14px",
                              border: `1px solid ${theme.border}`, overflow: "auto",
                            }}>
                              <div style={{ fontSize: "9px", color: theme.textMuted, letterSpacing: "1px", marginBottom: "8px" }}>
                                PYTHON IMPLEMENTATION
                              </div>
                              <pre style={{
                                fontSize: "11px", color: theme.cyan, margin: 0,
                                whiteSpace: "pre-wrap", wordBreak: "break-word", lineHeight: 1.6,
                                fontFamily: theme.font,
                              }}>{rec.python_snippet}</pre>
                            </div>
                          )}
                        </div>
                      );
                    })}

                    {/* Advanced Pipeline */}
                    {analysisResult.advanced_pipeline && (
                      <div style={{
                        background: `linear-gradient(135deg, rgba(200,169,110,0.08), rgba(78,205,196,0.05))`,
                        border: `1px solid ${theme.gold}`,
                        borderRadius: "12px", padding: "20px", marginTop: "16px",
                      }}>
                        <h4 style={{ fontSize: "12px", color: theme.gold, letterSpacing: "1px", margin: "0 0 12px" }}>
                          🚀 ADVANCED END-TO-END PIPELINE
                        </h4>
                        <div style={{ fontSize: "12px", color: theme.textDim, lineHeight: 1.8 }}>
                          {analysisResult.advanced_pipeline}
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {analysisResult?.error && (
                  <div style={{
                    background: theme.card, border: `1px solid ${theme.red}`,
                    borderRadius: "10px", padding: "20px", marginTop: "16px",
                  }}>
                    <div style={{ fontSize: "13px", color: theme.red, fontWeight: 700, marginBottom: "8px" }}>Analysis Error</div>
                    <div style={{ fontSize: "11px", color: theme.textDim }}>{analysisResult.error}</div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </main>
      
      {/* Footer */}
      <footer style={{
        padding: "16px 24px", borderTop: `1px solid ${theme.border}`,
        textAlign: "center", fontSize: "10px", color: theme.textMuted,
      }}>
        OptiML Engine — {TECHNIQUES.optimization.length + TECHNIQUES.ml.length + TECHNIQUES.operations_research.length} Techniques × {INDUSTRIES.length} Industries — Powered by Claude AI
      </footer>
    </div>
  );
}
