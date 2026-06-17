// VAT Analytics — UI-logik. Ekstern fil (ingen inline <script>/onclick), så
// CSP kan være stram. Dynamiske klik håndteres via event-delegation.
(function () {
  "use strict";

  const CSRF = (document.querySelector('meta[name="csrf-token"]') || {}).content || "";

  const uploadArea = document.getElementById("upload-area");
  const fileInput = document.getElementById("file-input");
  const progressSection = document.getElementById("progress-section");
  const resultsSection = document.getElementById("results-section");
  const errorSection = document.getElementById("error-section");
  const container = document.getElementById("categories-container");

  let pollInterval = null;

  function escapeHtml(str) {
    if (str == null) return "";
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatFileSize(bytes) {
    if (bytes >= 1024 * 1024 * 1024) return (bytes / (1024 * 1024 * 1024)).toFixed(1) + " GB";
    if (bytes >= 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
    if (bytes >= 1024) return (bytes / 1024).toFixed(1) + " KB";
    return bytes + " bytes";
  }

  // Drag & drop
  uploadArea.addEventListener("dragover", (e) => { e.preventDefault(); uploadArea.classList.add("dragover"); });
  uploadArea.addEventListener("dragleave", () => { uploadArea.classList.remove("dragover"); });
  uploadArea.addEventListener("drop", (e) => {
    e.preventDefault(); uploadArea.classList.remove("dragover");
    if (e.dataTransfer.files[0]) uploadFile(e.dataTransfer.files[0]);
  });
  fileInput.addEventListener("change", () => { if (fileInput.files[0]) uploadFile(fileInput.files[0]); });

  function fmt(amount, currency) {
    currency = currency || "DKK";
    return `${escapeHtml(currency)} ${amount.toLocaleString("da-DK", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  }

  function updateProgressBar(percent, detail, fileInfo) {
    const wrapper = document.getElementById("progress-bar-wrapper");
    const fill = document.getElementById("progress-bar-fill");
    const text = document.getElementById("progress-bar-text");
    const detailEl = document.getElementById("progress-detail");
    const fileInfoEl = document.getElementById("progress-file-info");

    wrapper.classList.remove("hidden");
    fill.style.width = percent + "%";
    text.textContent = percent + "%";
    if (detail) detailEl.textContent = detail;
    if (fileInfo) fileInfoEl.textContent = fileInfo;
  }

  function hideProgressBar() {
    document.getElementById("progress-bar-wrapper").classList.add("hidden");
  }

  function stopPolling() {
    if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
  }

  function pollJobStatus(jobId, fileSize) {
    const statusTexts = {
      queued: "Fil modtaget, venter på processering...",
      parsing: "Parser data fra filen...",
      analyzing: "Kører 103 momsanalyser...",
    };

    pollInterval = setInterval(async () => {
      try {
        const resp = await fetch("/status/" + encodeURIComponent(jobId));
        if (!resp.ok) {
          const err = await resp.json();
          stopPolling();
          showError(err.detail || "Fejl ved statuscheck");
          return;
        }
        const status = await resp.json();

        const progressText = document.getElementById("progress-text");
        progressText.textContent = statusTexts[status.status] || "Arbejder...";

        if (status.status === "parsing" || status.status === "analyzing") {
          let detail = "";
          if (status.rows_processed > 0 && status.total_rows > 0) {
            detail = escapeHtml(status.rows_processed.toLocaleString("da-DK"))
              + " af " + escapeHtml(status.total_rows.toLocaleString("da-DK"))
              + " rækker behandlet";
          }
          let fileInfo = "Filstørrelse: " + formatFileSize(status.file_size);
          updateProgressBar(status.progress, detail, fileInfo);
        }

        if (status.status === "done") {
          stopPolling();
          updateProgressBar(100, "Henter resultater...", "");
          const resultResp = await fetch("/result/" + encodeURIComponent(jobId));
          if (!resultResp.ok) {
            const err = await resultResp.json();
            showError(err.detail || "Fejl ved hentning af resultater");
            return;
          }
          const data = await resultResp.json();
          showResults(data);
        }

        if (status.status === "error") {
          stopPolling();
          showError(status.error || "Ukendt fejl under analyse");
        }
      } catch (err) {
        console.error("Poll error:", err);
      }
    }, 2000);
  }

  async function uploadFile(file) {
    showSection("progress");
    hideProgressBar();
    stopPolling();

    const progressText = document.getElementById("progress-text");
    progressText.textContent = "Uploader " + escapeHtml(file.name) + " (" + formatFileSize(file.size) + ")...";

    const formData = new FormData();
    formData.append("file", file);
    try {
      const headers = CSRF ? { "X-CSRF-Token": CSRF } : {};
      const response = await fetch("/analyze", { method: "POST", body: formData, headers });
      const data = await response.json();

      if (!response.ok) {
        showError(data.detail || data.error || "Ukendt fejl");
        return;
      }

      if (data.job_id) {
        progressText.textContent = data.message || "Stor fil — analyse kører i baggrunden...";
        updateProgressBar(0, "Venter på processering...", "Filstørrelse: " + formatFileSize(data.file_size));
        pollJobStatus(data.job_id, data.file_size);
        return;
      }

      showResults(data);
    } catch (err) {
      showError("Kunne ikke forbinde til serveren. Prøv igen.");
    }
  }

  function showResults(data) {
    const a = data.analytics;

    const score = a.overall_score;
    const scoreCircle = document.getElementById("score-circle");
    document.getElementById("score-number").textContent = score;
    scoreCircle.className = "score-circle " + (score >= 80 ? "score-good" : score >= 50 ? "score-warning" : "score-bad");
    document.getElementById("score-summary").textContent =
      `${a.total_findings} findings fundet på tværs af ${a.categories.filter(c => c.findings_count > 0).length} kategorier`;

    const econ = a.impact_summary.economic;
    const interest = a.impact_summary.interest_risk;
    const comp = a.impact_summary.compliance;
    const cur = econ.currency || "DKK";

    // Distinkt (transaktions-dedupliceret) net ved siden af brutto-net.
    function setDistinct(elId, imp) {
      const el = document.getElementById(elId);
      if (!el) return;
      if (imp.net_amount_distinct == null) { el.textContent = ""; return; }
      el.textContent = `Distinkt net: ${fmt(imp.net_amount_distinct, cur)} · ${imp.distinct_transactions || 0} transaktioner`;
    }

    document.getElementById("econ-negative").textContent = fmt(econ.negative_amount, cur);
    document.getElementById("econ-positive").textContent = fmt(econ.positive_amount, cur);
    document.getElementById("econ-net").textContent = fmt(econ.net_amount, cur);
    document.getElementById("econ-net").className = econ.net_amount >= 0 ? "net-positive" : "net-negative";
    document.getElementById("econ-count").textContent = `${econ.total_findings} findings`;
    setDistinct("econ-distinct", econ);

    document.getElementById("interest-negative").textContent = fmt(interest.negative_amount, cur);
    document.getElementById("interest-positive").textContent = fmt(interest.positive_amount, cur);
    document.getElementById("interest-net").textContent = fmt(interest.net_amount, cur);
    document.getElementById("interest-net").className = interest.net_amount >= 0 ? "net-positive" : "net-negative";
    document.getElementById("interest-count").textContent = `${interest.total_findings} findings`;
    setDistinct("interest-distinct", interest);

    const compBars = document.getElementById("compliance-bars");
    compBars.innerHTML = `
      ${comp.critical_count ? `<span class="sev-badge sev-critical">${escapeHtml(comp.critical_count)} kritiske</span>` : ""}
      ${comp.high_count ? `<span class="sev-badge sev-high">${escapeHtml(comp.high_count)} høj</span>` : ""}
      ${comp.medium_count ? `<span class="sev-badge sev-medium">${escapeHtml(comp.medium_count)} medium</span>` : ""}
      ${comp.low_count ? `<span class="sev-badge sev-low">${escapeHtml(comp.low_count)} lav</span>` : ""}
      ${comp.total_findings === 0 ? '<span class="no-findings">Ingen findings</span>' : ""}
    `;
    document.getElementById("compliance-count").textContent = `${comp.total_findings} findings`;

    const sev = a.severity_summary;
    document.getElementById("sev-critical").textContent = `${sev.critical} kritiske`;
    document.getElementById("sev-high").textContent = `${sev.high} høj`;
    document.getElementById("sev-medium").textContent = `${sev.medium} medium`;
    document.getElementById("sev-low").textContent = `${sev.low} lav`;
    document.getElementById("sev-total").textContent = `${a.total_findings} findings i alt`;

    container.innerHTML = "";
    for (const cat of a.categories) {
      const scoreClass = cat.score >= 80 ? "cat-good" : cat.score >= 50 ? "cat-warning" : "cat-bad";
      const div = document.createElement("div");
      div.className = "category-card";
      div.innerHTML = `
        <div class="cat-header">
          <div class="cat-score-mini ${scoreClass}">${escapeHtml(cat.score)}</div>
          <div class="cat-info">
            <span class="cat-name">${escapeHtml(cat.name)}</span>
            <span class="cat-meta">${escapeHtml(cat.total_tests)} tests · ${escapeHtml(cat.findings_count)} findings</span>
          </div>
          <div class="cat-severity">
            ${cat.critical_count ? `<span class="sev-dot sev-critical-dot">${escapeHtml(cat.critical_count)}</span>` : ""}
            ${cat.high_count ? `<span class="sev-dot sev-high-dot">${escapeHtml(cat.high_count)}</span>` : ""}
            ${cat.medium_count ? `<span class="sev-dot sev-medium-dot">${escapeHtml(cat.medium_count)}</span>` : ""}
            ${cat.low_count ? `<span class="sev-dot sev-low-dot">${escapeHtml(cat.low_count)}</span>` : ""}
          </div>
          <span class="cat-toggle">▼</span>
        </div>
        <div class="cat-body">
          ${cat.findings.length === 0 ? '<p class="no-findings-text">Ingen findings — alle tests bestået</p>' : ""}
          ${cat.findings.map(f => `
            <div class="finding finding-${escapeHtml(f.severity)}">
              <div class="finding-header">
                <span class="finding-severity sev-${escapeHtml(f.severity)}">${escapeHtml(f.severity.toUpperCase())}</span>
                <span class="finding-direction">${f.direction === "negative" ? "⬇️" : f.direction === "positive" ? "⬆️" : "➡️"}</span>
                <span class="finding-test">Test ${escapeHtml(f.test_id)}: ${escapeHtml(f.test_name)}</span>
              </div>
              <p class="finding-desc">${escapeHtml(f.description)}</p>
              ${f.estimated_amount ? `<p class="finding-amount ${f.direction === "positive" ? "amt-positive" : "amt-negative"}">${fmt(f.estimated_amount, cur)}</p>` : ""}
              ${f.fix_suggestion ? `
              <div class="fix-suggestion">
                <div class="fix-header">
                  <span class="fix-icon">💡</span>
                  <span class="fix-label">Vis løsningsforslag</span>
                  <span class="fix-toggle">▼</span>
                </div>
                <div class="fix-body">
                  <p class="fix-text">${escapeHtml(f.fix_suggestion)}</p>
                </div>
              </div>
              ` : ""}
              ${f.transactions && f.transactions.length > 0 ? `
              <div class="transactions-toggle">
                Vis ${escapeHtml(f.transactions.length)} berørte transaktioner
              </div>
              <div class="transactions-table hidden">
                <table>
                  <thead><tr>
                    <th>ID</th><th>Dato</th><th>Konto</th><th>Beløb</th><th>Moms</th><th>Problem</th>
                  </tr></thead>
                  <tbody>
                    ${f.transactions.map(t => `
                      <tr>
                        <td><code>${escapeHtml(t.transaction_id || "-")}</code></td>
                        <td>${escapeHtml(t.date || "-")}</td>
                        <td>${escapeHtml(t.account_id || "-")}</td>
                        <td>${t.amount ? fmt(t.amount, cur) : "-"}</td>
                        <td>${t.vat_recorded != null ? fmt(t.vat_recorded, cur) : "-"}</td>
                        <td class="highlight">${escapeHtml(t.highlighted_field || "-")}</td>
                      </tr>
                    `).join("")}
                  </tbody>
                </table>
              </div>
              ` : ""}
            </div>
          `).join("")}
        </div>
      `;
      container.appendChild(div);
    }

    showSection("results");
  }

  function showError(message) {
    stopPolling();
    document.getElementById("error-text").textContent = message;
    showSection("error");
  }

  function showSection(section) {
    uploadArea.classList.toggle("hidden", section !== "upload");
    progressSection.classList.toggle("hidden", section !== "progress");
    resultsSection.classList.toggle("hidden", section !== "results");
    errorSection.classList.toggle("hidden", section !== "error");
  }

  function reset() {
    stopPolling();
    hideProgressBar();
    fileInput.value = "";
    showSection("upload");
  }

  document.getElementById("reset-btn").addEventListener("click", reset);
  document.getElementById("error-reset-btn").addEventListener("click", reset);

  // Event-delegation for dynamisk genererede paneler (erstatter inline onclick).
  container.addEventListener("click", (e) => {
    const fixHeader = e.target.closest(".fix-header");
    if (fixHeader && container.contains(fixHeader)) {
      fixHeader.parentElement.classList.toggle("fix-expanded");
      return;
    }
    const txToggle = e.target.closest(".transactions-toggle");
    if (txToggle && container.contains(txToggle)) {
      txToggle.nextElementSibling.classList.toggle("hidden");
      return;
    }
    const catHeader = e.target.closest(".cat-header");
    if (catHeader && container.contains(catHeader)) {
      catHeader.parentElement.classList.toggle("expanded");
    }
  });
})();
