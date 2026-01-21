const express = require("express");
const path = require("path");

const app = express();

// この server.js があるフォルダの「1つ上」を公開
const publicDir = path.resolve(__dirname, "..");

// / で ../ を丸ごと静的配信
app.use(express.static(publicDir, {
  index: false,      // index.html 自動表示しない（お好みで true に）
  dotfiles: "deny",  // .env とかドットファイルは拒否（最低限のガード）
}));

app.get("/", (req, res) => {
  res.type("text").send(`Serving: ${publicDir}\nTry: /somefile.ext`);
});

const port = process.env.PORT || 3000;
app.listen(port, "0.0.0.0", () => {
  console.log(`http://localhost:${port}`);
  console.log(`Serving directory: ${publicDir}`);
});
