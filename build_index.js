// build_index.js
//
// Scan vine_archive_harvest/posts/<userId>/<postId>.json
// and build a compact index file: data/posts_index.json
//
// Run this whenever you add new harvested posts:
//    node build_index.js

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");

const ROOT = __dirname;
const POSTS_ROOT = path.join(ROOT, "vine_archive_harvest", "posts");
const DATA_DIR = path.join(ROOT, "data");
const INDEX_FILE = path.join(DATA_DIR, "posts_index.json");

async function main() {
    console.log("Building index from:", POSTS_ROOT);
    await fsp.mkdir(DATA_DIR, { recursive: true });

    const posts = [];

    // helper for created date
    function parseCreated(created) {
        if (!created) return 0;
        // handles "2016-07-31T22:08:15.000000"
        const match = created.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})/);
        let s = created;
        if (match) s = match[1] + "Z";
        const ts = Date.parse(s);
        return Number.isNaN(ts) ? 0 : ts;
    }

    // read user directories
    const userDirs = await fsp.readdir(POSTS_ROOT, { withFileTypes: true });
    for (const udir of userDirs) {
        if (!udir.isDirectory()) continue;
        const userId = udir.name;
        const userPath = path.join(POSTS_ROOT, userId);
        let files;
        try {
            files = await fsp.readdir(userPath, { withFileTypes: true });
        } catch (e) {
            console.warn("Failed to read", userPath, e.message);
            continue;
        }

        for (const f of files) {
            if (!f.isFile()) continue;
            if (!f.name.endsWith(".json")) continue;

            const postId = f.name.replace(/\.json$/, "");
            const fullPath = path.join(userPath, f.name);

            let raw;
            try {
                raw = await fsp.readFile(fullPath, "utf8");
            } catch {
                continue;
            }

            let json;
            try {
                json = JSON.parse(raw);
            } catch {
                continue;
            }

            const created =
                json.created || json.created_at || json.creationDate || "";

            const rec = {
                userId: String(json.userIdStr || json.userId || userId),
                postId: String(json.postIdStr || json.postId || postId),
                username: json.username || json.author || "",
                description:
                    json.description ||
                    json.descriptionPlain ||
                    (json.caption && json.caption.text) ||
                    "",
                thumbnailUrl: json.thumbnailUrl || null,
                created,
                createdTs: parseCreated(created),
                loops: json.loops || json.loopCount || 0,
                likes: json.likes || json.likeCount || 0,
                comments: json.comments || json.commentCount || 0,
                reposts: json.reposts || json.repostCount || 0,
            };

            posts.push(rec);
        }
    }

    console.log("Posts scanned:", posts.length);

    // newest first for feed
    posts.sort((a, b) => b.createdTs - a.createdTs);

    await fsp.writeFile(INDEX_FILE, JSON.stringify(posts, null, 2));
    console.log("Index written to:", INDEX_FILE);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
