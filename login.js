// Simple client-side login handling (demo only)
document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("loginForm");
  const err = document.getElementById("error");

  form.addEventListener("submit", (ev) => {
    ev.preventDefault();
    err.textContent = "";

    const username = document.getElementById("username").value.trim();
    const password = document.getElementById("password").value.trim();

    if (!username || !password) {
      err.textContent = "Please enter both username and password.";
      return;
    }

  // Demo authentication: accept any non-empty credentials.
  // Store username and password in sessionStorage (demo only).
  // NOTE: Storing passwords in plain sessionStorage is insecure and
  // should only be used for local demos. For production use an
  // authenticated server-issued token instead.
  sessionStorage.setItem("hf_username", username);
  sessionStorage.setItem("hf_password", password);
  // Redirect to the app endpoint which serves the original index.html
  window.location.href = "/app";
  });
});