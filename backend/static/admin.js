// Admin-side: kopiér invitationslink + bekræft sletning.
// Ekstern fil (ingen inline onclick/onsubmit), så CSP forbliver stram.
(function () {
  "use strict";

  // Kopiér-knapper: <button data-copy-target="element-id">
  document.querySelectorAll("[data-copy-target]").forEach(function (btn) {
    btn.addEventListener("click", function () {
      var el = document.getElementById(btn.getAttribute("data-copy-target"));
      if (!el) return;
      var text = el.textContent;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(function () {
          btn.textContent = "Kopieret ✓";
        });
      }
    });
  });

  // Bekræft farlige handlinger: <form data-confirm="Er du sikker?">
  document.querySelectorAll("form[data-confirm]").forEach(function (form) {
    form.addEventListener("submit", function (e) {
      if (!window.confirm(form.getAttribute("data-confirm"))) {
        e.preventDefault();
      }
    });
  });
})();
