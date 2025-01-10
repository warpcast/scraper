import { Cluster } from "ioredis";
import puppeteer from "puppeteer";

// Cache browsers by language
const browsers = new Map();

let totalScrapeAttempts = 0;
let stopped = false;

const REDIS_URL = new URL(process.env.REDIS_URL || "redis://localhost:6379");
const WAIT_QUEUE = process.env.WAIT_QUEUE || "scrape-jobs:wait";
const DONE_QUEUE = process.env.DONE_QUEUE || "scrape-jobs:done";
// Hack, but works for our purposes
const TLS = REDIS_URL.hostname.includes("amazon");

const redis = new Cluster(
	[{ host: REDIS_URL.hostname, port: REDIS_URL.port }],
	{
		enableReadyCheck: true,
		enableOfflineQueue: false,
		scaleReads: "master", // consistent reads always
		slotsRefreshTimeout: 60_000, // 1 minute

		// Default options passed to the individual Redis nodes constructor
		redisOptions: {
			username: REDIS_URL.username,
			password: REDIS_URL.password,
			enableReadyCheck: true,
			tls: TLS
				? {
						checkServerIdentity: () => {
							// skip cert validation (needed for AWS ElastiCache)
							return undefined;
						},
					}
				: undefined,
		},
		// Needed when running in AWS
		// https://github.com/redis/ioredis/issues/1003#issuecomment-599074145
		dnsLookup: TLS ? (address, callback) => callback(null, address) : undefined,
	},
);

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getHTML(url, language) {
	totalScrapeAttempts += 1;
	let browser = null;
	if (browsers.has(language)) {
		browser = browsers.get(language);
	} else {
		browser = await puppeteer.launch({
			headless: false,
			args: [`--accept-lang=${language}`, "--incognito"],
		});
		browsers.set(language, browser);

		// Ensure all pages are closed first
		// (since it starts off with a blank page opened)
		for (const page of await browser.pages()) {
			await page.close();
		}
	}

	let page = null;
	try {
		page = await browser.newPage();
		console.log("Scraping", url);
		await page.goto(url);
		const html = await page.content();
		return html;
	} finally {
		if (page) {
			// Close page and delete cookies async so we don't block
			// returning the HTML on cleanup
			page
				.close()
				.then(() => browser.cookies())
				.then((cookies) => browser.deleteCookie(...cookies))
				.catch((error) => {
					console.log("Ignoring cleanup error", error);
				});
		}
	}
}

redis.on("ready", async () => {
	try {
		while (!stopped) {
			try {
				console.log("Waiting for next job...");
				const [_queue, rawJobStr] = await redis.blpop(WAIT_QUEUE, 0);
				const job = JSON.parse(rawJobStr);
				if ("id" in job && "url" in job) {
					try {
						const html = await getHTML(job.url, job.lang || "en-US");
						job.html = html;
						job.success = true;
					} catch (e) {
						console.error("Unable to scrape URL", url, e);
						job.success = false;
					}
					try {
						await redis.rpush(DONE_QUEUE, JSON.stringify(job));
					} catch (e) {
						console.error(
							`Unable to store job result for ID ${job.id}. Re-enqueueing job...`,
						);
						await redis.rpush(WAIT_QUEUE, rawJobStr);
					}
				} else {
					console.warn(`Ignoring invalid scrape job: ${rawJobStr}`);
				}
			} catch (e) {
				console.error("Error fetching scrape job", e);
				await sleep(500);
			}
		}
	} finally {
		for (const [_lang, browser] of browsers.entries()) {
			await browser.close();
		}
	}
});

for (const code of ["SIGINT", "SIGQUIT", "SIGTERM", "exit"]) {
	process.on(code, (signal) => {
		redis.quit();
		stopped = true;
		console.log("Exiting process due to signal", signal);
		process.exit(signal);
	});
}
