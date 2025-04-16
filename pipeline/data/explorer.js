document.addEventListener("DOMContentLoaded", function () {
    const rootPath = "./dl0/";
    const container = document.getElementById("explorer");
  
    async function loadFolders() {
      try {
        const res = await fetch(rootPath);
        const text = await res.text();
  
        // Estrae i nomi delle directory dall'elenco (funziona con server tipo Python http.server)
        const parser = new DOMParser();
        const html = parser.parseFromString(text, "text/html");
        const links = Array.from(html.querySelectorAll("a"))
          .map(a => a.getAttribute("href"))
          .filter(href => href.endsWith("/") && href !== "../");
  
        for (const folder of links) {
          const folderPath = rootPath + folder;
          const div = document.createElement("div");
          div.innerHTML = `<h3>${folder.replace(/\/$/, "")}</h3><ul id="${folder}-list">Loading...</ul>`;
          container.appendChild(div);
          await loadFolderContent(folderPath, `${folder}-list`);
        }
  
      } catch (err) {
        container.innerHTML = `<p>Errore nel caricamento delle cartelle: ${err}</p>`;
      }
    }
  
    async function loadFolderContent(folderPath, listId) {
      try {
        const res = await fetch(folderPath);
        const text = await res.text();
        const parser = new DOMParser();
        const html = parser.parseFromString(text, "text/html");
        const files = Array.from(html.querySelectorAll("a"))
          .map(a => a.getAttribute("href"))
          .filter(name => name.endsWith(".pdf"));
  
        const ul = document.getElementById(listId);
        ul.innerHTML = files.length
          ? files.map(file => `<li><a href="${folderPath + file}" target="_blank">${file}</a></li>`).join("")
          : "<li><em>Nessun PDF trovato</em></li>";
      } catch (err) {
        document.getElementById(listId).innerHTML = `<li>Errore nel caricamento: ${err}</li>`;
      }
    }
  
    loadFolders();
  });