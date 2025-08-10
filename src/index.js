import fs from 'fs';
import path from 'path';
import http from 'http';
import axios from 'axios';
import dotenv from 'dotenv';
import { Client, GatewayIntentBits, EmbedBuilder, AttachmentBuilder } from 'discord.js';

dotenv.config();

// Configuration and constants
const ROOT_DIR = path.resolve(process.cwd());
const DATA_DIR = path.join(ROOT_DIR, 'data');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const CONFIG_FILE = path.join(ROOT_DIR, 'config.json');
const PUBLIC_PORT = process.env.PUBLIC_PORT ? Number(process.env.PUBLIC_PORT) : 8788;

// Simple structured logger
function log(level, message, meta = {}) { 
  const ts = new Date().toISOString();
  const flatMeta = Object.entries(meta)
    .map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`)
    .join(' ');
  const line = flatMeta ? `[${ts}] [${level}] ${message} ${flatMeta}` : `[${ts}] [${level}] ${message}`;
  console.log(line);
}

// Utilities
function ensureDirectoryExists(directoryPath) {
  if (!fs.existsSync(directoryPath)) {
    fs.mkdirSync(directoryPath, { recursive: true });
  }
}

function readJsonFile(filePath, fallback) {
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const text = raw.replace(/^\uFEFF/, '');
    return JSON.parse(text);
  } catch (err) {
    return fallback;
  }
}

function writeJsonFile(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function formatUtcDateToDisplay(date) {
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  const hh = String(date.getUTCHours()).padStart(2, '0');
  const mi = String(date.getUTCMinutes()).padStart(2, '0');
  const ss = String(date.getUTCSeconds()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function getConfigSignature(config) {
  const shallow = {
    startTimeUtc: config.startTimeUtc,
    endTimeUtc: config.endTimeUtc,
    targetTotalUsd: config.targetTotalUsd,
    minPerHour: config.minPerHour,
    maxPerHour: config.maxPerHour,
    amountPerMessageUsd: config.amountPerMessageUsd,
    tokensPerHundred: config.tokensPerHundred,
    enforceHardStopAtEnd: config.enforceHardStopAtEnd,
    specialPhase: config.specialPhase ? {
      enabled: !!config.specialPhase.enabled,
      countdownStartUtc: config.specialPhase.countdownStartUtc,
      presaleStartUtc: config.specialPhase.presaleStartUtc,
      initialBurstMinutes: config.specialPhase.initialBurstMinutes,
      initialBurstCount: config.specialPhase.initialBurstCount
    } : null
  };
  return JSON.stringify(shallow);
}

// Time management with one-time internet sync
let timeOffset = null;

async function initializeTimeOffset() {
  if (timeOffset !== null) return;
  
  log('INFO', 'Initializing time offset with internet UTC');
  const systemTime = new Date();
  
  try {
    log('INFO', 'Attempting to fetch time from worldtimeapi.org');
    const res = await axios.get('https://worldtimeapi.org/api/timezone/Etc/UTC', { timeout: 8000 });
    const internetTime = new Date(res.data.utc_datetime);
    timeOffset = internetTime.getTime() - systemTime.getTime();
    log('INFO', 'Successfully synchronized with worldtimeapi.org', { 
      systemTime: systemTime.toISOString(),
      internetTime: internetTime.toISOString(),
      offsetMs: timeOffset
    });
    return;
  } catch (e) {
    log('WARN', 'Failed to fetch time from worldtimeapi.org', { 
      error: e.message, 
      code: e.code
    });
  }
  
  timeOffset = 0;
  log('WARN', 'Using system time as fallback', {
    systemTime: systemTime.toISOString()
  });
}

function getCurrentUtcTime() {
  if (timeOffset === null) {
    throw new Error('Time offset not initialized. Call initializeTimeOffset() first.');
  }
  return new Date(Date.now() + timeOffset);
}

// Discord message sending
async function sendDiscordMessage(client, channelId, content, embeds = [], files = []) {
  try {
    log('INFO', 'Attempting to fetch channel', { channelId });
    const channel = await client.channels.fetch(channelId);
    if (!channel) {
      throw new Error('Channel not found');
    }
    
    log('INFO', 'Channel found, checking permissions', { 
      channelName: channel.name,
      channelType: channel.type,
      guildId: channel.guildId
    });
    
    // Check bot permissions
    if (channel.guild) {
      const botMember = channel.guild.members.cache.get(client.user.id);
      const permissions = channel.permissionsFor(botMember);
      log('INFO', 'Bot permissions in channel', {
        canSendMessages: permissions.has('SendMessages'),
        canEmbedLinks: permissions.has('EmbedLinks'),
        canAttachFiles: permissions.has('AttachFiles'),
        canViewChannel: permissions.has('ViewChannel')
      });
    }
    
    const messageOptions = { content };
    if (embeds.length > 0) messageOptions.embeds = embeds;
    if (files.length > 0) messageOptions.files = files;
    
    await channel.send(messageOptions);
    log('INFO', 'Message sent successfully');
  } catch (error) {
    log('ERROR', 'Failed to send Discord message', { 
      error: error.message,
      code: error.code,
      channelId 
    });
    throw error;
  }
}

// Schedule generation functions (same as Telegram version)
function sampleUniqueSecondsWithinHour(count) {
  const max = 3600;
  if (count >= max) {
    return Array.from({ length: max }, (_, i) => i);
  }
  const picked = new Set();
  while (picked.size < count) {
    picked.add(Math.floor(Math.random() * max));
  }
  return Array.from(picked).sort((a, b) => a - b);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function floorToHourTs(timestampMs) {
  const d = new Date(timestampMs);
  d.setUTCMinutes(0, 0, 0);
  return d.getTime();
}

function secondOfHour(timestampMs) {
  return Math.floor((timestampMs - floorToHourTs(timestampMs)) / 1000);
}

function enforceUniqueIntervals(scheduleIsoArray) {
  if (scheduleIsoArray.length <= 1) return scheduleIsoArray;
  const times = scheduleIsoArray.map(iso => new Date(iso).getTime());
  const usedIntervals = new Set();
  const hourToSeconds = new Map();

  for (const t of times) {
    const hourKey = floorToHourTs(t);
    const sec = secondOfHour(t);
    if (!hourToSeconds.has(hourKey)) hourToSeconds.set(hourKey, new Set());
    hourToSeconds.get(hourKey).add(sec);
  }

  for (let i = 1; i < times.length; i += 1) {
    const prev = times[i - 1];
    let curr = times[i];
    let deltaSec = Math.max(1, Math.round((curr - prev) / 1000));
    if (!usedIntervals.has(deltaSec)) {
      usedIntervals.add(deltaSec);
      continue;
    }

    const hourKey = floorToHourTs(curr);
    const hourStart = hourKey;
    const hourEnd = hourStart + 3600_000 - 1;
    const lowerBound = Math.max(hourStart, prev + 1000);
    const upperBound = (i + 1 < times.length)
      ? Math.min(hourEnd, times[i + 1] - 1000)
      : hourEnd;
    if (lowerBound > upperBound) {
      usedIntervals.add(deltaSec);
      continue;
    }

    const usedSecsInHour = hourToSeconds.get(hourKey) || new Set();
    const currentSecOfHour = secondOfHour(curr);
    usedSecsInHour.delete(currentSecOfHour);

    let found = false;
    const radiusLimit = 300;
    const baseSec = secondOfHour(curr);
    const lbSec = Math.ceil((lowerBound - hourStart) / 1000);
    const ubSec = Math.floor((upperBound - hourStart) / 1000);
    for (let r = 1; r <= radiusLimit && !found; r += 1) {
      const candidates = [];
      const plus = baseSec + r;
      const minus = baseSec - r;
      if (plus <= ubSec) candidates.push(plus);
      if (minus >= lbSec) candidates.push(minus);
      for (const candSec of candidates) {
        if (usedSecsInHour.has(candSec)) continue;
        const candTs = hourStart + candSec * 1000;
        const candDelta = Math.max(1, Math.round((candTs - prev) / 1000));
        if (usedIntervals.has(candDelta)) continue;
        curr = candTs;
        deltaSec = candDelta;
        found = true;
        break;
      }
    }

    if (!found) {
      for (let sec = lbSec; sec <= ubSec; sec += 1) {
        if (usedSecsInHour.has(sec)) continue;
        const candTs = hourStart + sec * 1000;
        const candDelta = Math.max(1, Math.round((candTs - prev) / 1000));
        if (usedIntervals.has(candDelta)) continue;
        curr = candTs;
        deltaSec = candDelta;
        found = true;
        break;
      }
    }

    times[i] = curr;
    usedIntervals.add(deltaSec);
    usedSecsInHour.add(secondOfHour(curr));
    hourToSeconds.set(hourKey, usedSecsInHour);
  }

  return times.map(ts => new Date(ts).toISOString());
}

function generatePerHourWeights(startIso, hours) {
  const weights = [];
  for (let i = 0; i < hours; i += 1) {
    const d = new Date(new Date(startIso).getTime() + i * 3600_000);
    const hour = d.getUTCHours();
    const day = d.getUTCDay();
    const diurnal = 0.95 + 0.35 * Math.sin((2 * Math.PI * (hour + 2)) / 24);
    const weekdayBias = [0.9, 0.95, 1.0, 1.05, 1.1, 1.2, 1.25][day];
    const noise = 0.9 + Math.random() * 0.3;
    weights.push(diurnal * weekdayBias * noise);
  }
  return weights;
}

function generateSchedule(config) {
  const start = new Date(config.startTimeUtc);
  const desiredEnd = new Date(config.endTimeUtc);
  const amountPerMessage = config.amountPerMessageUsd;
  const totalMessagesNeeded = Math.ceil(config.targetTotalUsd / amountPerMessage);

  const initialHours = Math.ceil((desiredEnd.getTime() - start.getTime()) / 3600_000);
  const minPerHour = config.minPerHour;
  const maxPerHour = config.maxPerHour;

  let hours = Math.max(1, initialHours);
  const maxPossible = hours * maxPerHour;
  if (maxPossible < totalMessagesNeeded) {
    const additionalHours = Math.ceil((totalMessagesNeeded - maxPossible) / maxPerHour);
    hours += additionalHours;
  }

  const weights = generatePerHourWeights(start.toISOString(), hours);
  const sumWeights = weights.reduce((a, b) => a + b, 0);

  let perHour = weights.map(w => Math.round((w / sumWeights) * totalMessagesNeeded));

  let currentTotal = perHour.reduce((a, b) => a + b, 0);
  if (currentTotal !== totalMessagesNeeded) {
    perHour = perHour.map(n => Math.min(n, maxPerHour));
    currentTotal = perHour.reduce((a, b) => a + b, 0);
    let diff = totalMessagesNeeded - currentTotal;
    if (diff > 0) {
      const idxs = Array.from({ length: hours }, (_, i) => i).sort(() => Math.random() - 0.5);
      for (const i of idxs) {
        if (diff <= 0) break;
        const headroom = maxPerHour - perHour[i];
        if (headroom > 0) {
          const add = Math.min(headroom, diff);
          perHour[i] += add;
          diff -= add;
        }
      }
    } else if (diff < 0) {
      let remaining = -diff;
      const idxs = Array.from({ length: hours }, (_, i) => i).sort(() => Math.random() - 0.5);
      for (const i of idxs) {
        if (remaining <= 0) break;
        const reducible = Math.max(0, perHour[i] - 0);
        if (reducible > 0) {
          const r = Math.min(reducible, remaining);
          perHour[i] -= r;
          remaining -= r;
        }
      }
    }
  }

  const schedule = [];
  for (let h = 0; h < hours; h += 1) {
    const hourStart = new Date(start.getTime() + h * 3600_000);
    const seconds = sampleUniqueSecondsWithinHour(perHour[h]);
    for (const s of seconds) {
      const fireAt = new Date(hourStart.getTime() + s * 1000);
      schedule.push(fireAt.toISOString());
    }
  }
  schedule.sort();

  const uniqueSchedule = enforceUniqueIntervals(schedule);
  const effectiveEnd = new Date(start.getTime() + hours * 3600_000);

  return { schedule: uniqueSchedule, effectiveEnd: effectiveEnd.toISOString(), totalMessagesNeeded };
}

function sampleUniqueSeconds(count, spanSeconds) {
  const max = Math.max(1, spanSeconds);
  if (count >= max) {
    return Array.from({ length: max }, (_, i) => i);
  }
  const picked = new Set();
  while (picked.size < count) {
    picked.add(Math.floor(Math.random() * max));
  }
  return Array.from(picked).sort((a, b) => a - b);
}

function generateFullSchedule(config) {
  const amountPerMessage = config.amountPerMessageUsd;
  const totalMessagesNeeded = Math.ceil(config.targetTotalUsd / amountPerMessage);

  const entries = [];
  let remainingMessages = totalMessagesNeeded;
  let normalStartIso = config.startTimeUtc;

  if (config.specialPhase && config.specialPhase.enabled) {
    const countdownStart = new Date(config.specialPhase.countdownStartUtc);
    const presaleStart = new Date(config.specialPhase.presaleStartUtc);
    const initialBurstMinutes = config.specialPhase.initialBurstMinutes || 10;
    const initialBurstCount = config.specialPhase.initialBurstCount || 0;

    const countdownMessages = [
      { minutes: 59, image: '59.png' },
      { minutes: 30, image: '30.png' },
      { minutes: 15, image: '15.png' },
      { minutes: 5, image: '5.png' },
      { minutes: 4, image: '4.png' },
      { minutes: 3, image: '3.png' },
      { minutes: 2, image: '2.png' },
      { minutes: 1, image: '1.png' }
    ];
    
    for (const { minutes, image } of countdownMessages) {
      const t = new Date(presaleStart.getTime() - minutes * 60_000);
      const countdownText = `🚀 ${minutes} MINUTE${minutes !== 1 ? 'S' : ''} TO LAUNCH! 🚀

⚡ First Come, First Served — Only 14,000 spots!

👥 Whitelisted: 85,312
💰 Max: $100 each
💎 Supply: 10,000,000 $BBLP
📊 Hard Cap: $1.4M

📱 Follow: [Telegram](https://t.me/BblipProtocol_Annoucements)
🔗 Be ready: [bblip.io/presale](https://www.bblip.io/presale)`;
      entries.push({ at: t.toISOString(), kind: 'countdown', text: countdownText, image });
    }

    const launchText = `🎯 BBLIP PRESALE IS LIVE! 🎯

⚡ First Come, First Served — Only the first 14,000 investors get in.
👥 Whitelisted: 85,312
💰 Max: $100 each

💎 Supply: 10,000,000 $BBLP
📊 Hard Cap: $1,400,000

📱 Follow: [Telegram](https://t.me/BblipProtocol_Annoucements)
🔗 Secure your spot now: [bblip.io/presale](https://www.bblip.io/presale)`;
    entries.push({ at: presaleStart.toISOString(), kind: 'start', text: launchText, image: 'live.png' });

    if (initialBurstCount > 0) {
      const burstStart = presaleStart.getTime();
      const burstSpanSec = initialBurstMinutes * 60;
      const secs = sampleUniqueSeconds(initialBurstCount, burstSpanSec);
      for (const s of secs) {
        const t = new Date(burstStart + s * 1000);
        entries.push({ at: t.toISOString(), kind: 'buy' });
      }
      remainingMessages -= Math.min(initialBurstCount, remainingMessages);
      normalStartIso = new Date(burstStart + burstSpanSec * 1000).toISOString();
    } else {
      normalStartIso = presaleStart.toISOString();
    }
  }

  if (remainingMessages > 0) {
    const normalConfig = { ...config, startTimeUtc: normalStartIso, targetTotalUsd: remainingMessages * amountPerMessage };
    const { schedule, effectiveEnd } = generateSchedule(normalConfig);
    for (const iso of schedule) entries.push({ at: iso, kind: 'buy' });
    entries.sort((a, b) => new Date(a.at) - new Date(b.at));
    return { schedule: entries, effectiveEnd };
  }

  entries.sort((a, b) => new Date(a.at) - new Date(b.at));
  const lastAt = entries.length ? entries[entries.length - 1].at : config.startTimeUtc;
  return { schedule: entries, effectiveEnd: lastAt };
}

function formatMoney(amount) {
  return amount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function buildDiscordMessage(nowUtcDate, nextTotalUsd, tokensPerHundred) {
  const dateLine = formatUtcDateToDisplay(nowUtcDate);
  const tokensText = `${tokensPerHundred.toFixed(2)} $BBLP`;
  
  const totalInvestors = Math.floor(nextTotalUsd / 100);
  const totalTokensSold = totalInvestors * tokensPerHundred;
  const spotsRemaining = Math.max(0, 14000 - totalInvestors);
  
  const embed = new EmbedBuilder()
    .setColor(0x00ff00)
    .setTitle('BBLIP PRESALE [ PHASE 3 ]')
    .setDescription('🚀 **NEW PURCHASE!**')
    .addFields(
      { name: '💰 Amount', value: `$100.00 (${tokensText})`, inline: true },
      { name: '📅 Time', value: `${dateLine} UTC`, inline: true },
      { name: '\u200B', value: '\u200B', inline: true },
      { name: '📊 Raised', value: `$${formatMoney(nextTotalUsd)} / $1,400,000`, inline: true },
      { name: '💎 Sold', value: `${formatMoney(totalTokensSold)} / 10,000,000 $BBLP`, inline: true },
      { name: '\u200B', value: '\u200B', inline: true },
      { name: '👥 Spots Filled', value: `${totalInvestors.toLocaleString()} / 14,000`, inline: true },
      { name: '⚡ Remaining', value: spotsRemaining.toLocaleString(), inline: true },
      { name: '\u200B', value: '\u200B', inline: true },
      { name: '🔗 Links', value: '[X](https://x.com/BblipProtocol) • [Telegram](https://t.me/BblipProtocol_Annoucements) • [Whitepaper](http://bblip.io/whitepaper) • [Tokenomics](https://bblip.io/tokenomics) • [Buy Now](https://www.bblip.io/presale)', inline: false }
    )
    .setFooter({ 
      text: 'Bblip Protocol - Crypto to Spendable Currency',
      iconURL: 'https://bblip.io/favicon.ico'
    })
    .setTimestamp();

  return { embed, image: 'feed.png' };
}

async function main() {
  ensureDirectoryExists(DATA_DIR);
  await initializeTimeOffset();

  const fileConfig = readJsonFile(CONFIG_FILE, {});
  const envConfig = {
    BOT_TOKEN: process.env.BOT_TOKEN,
    TARGET_CHANNEL_ID: process.env.TARGET_CHANNEL_ID,
    startTimeUtc: process.env.START_TIME_UTC,
    endTimeUtc: process.env.END_TIME_UTC,
    targetTotalUsd: process.env.TARGET_TOTAL_USD ? Number(process.env.TARGET_TOTAL_USD) : undefined,
    amountPerMessageUsd: process.env.AMOUNT_PER_MESSAGE_USD ? Number(process.env.AMOUNT_PER_MESSAGE_USD) : undefined,
    tokensPerHundred: process.env.TOKENS_PER_HUNDRED ? Number(process.env.TOKENS_PER_HUNDRED) : undefined,
    startRaisedUsd: process.env.START_RAISED_USD ? Number(process.env.START_RAISED_USD) : undefined,
    minPerHour: process.env.MIN_PER_HOUR ? Number(process.env.MIN_PER_HOUR) : undefined,
    maxPerHour: process.env.MAX_PER_HOUR ? Number(process.env.MAX_PER_HOUR) : undefined,
    enforceHardStopAtEnd: process.env.ENFORCE_HARD_STOP === 'true'
  };

  const cleanEnvConfig = Object.fromEntries(
    Object.entries(envConfig).filter(([key, value]) => value !== undefined)
  );
  const merged = { ...fileConfig, ...cleanEnvConfig };
  const defaulted = {
    startTimeUtc: merged.startTimeUtc || '2025-08-08T17:45:00Z',
    endTimeUtc: merged.endTimeUtc || '2025-08-16T12:00:00Z',
    targetTotalUsd: merged.targetTotalUsd ?? 1_400_000,
    amountPerMessageUsd: merged.amountPerMessageUsd ?? 100,
    minPerHour: merged.minPerHour ?? 30,
    maxPerHour: merged.maxPerHour ?? 60,
    tokensPerHundred: merged.tokensPerHundred ?? 714.28,
    startRaisedUsd: merged.startRaisedUsd ?? 0,
    enforceHardStopAtEnd: merged.enforceHardStopAtEnd ?? false,
    BOT_TOKEN: merged.BOT_TOKEN,
    TARGET_CHANNEL_ID: merged.TARGET_CHANNEL_ID,
    specialPhase: merged.specialPhase
  };

  if (!defaulted.BOT_TOKEN || !defaulted.TARGET_CHANNEL_ID) {
    console.error('Please set BOT_TOKEN and TARGET_CHANNEL_ID in .env or config.json');
    process.exit(1);
  }

  // Initialize Discord client
  const client = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages
    ]
  });

  client.once('ready', () => {
    log('INFO', `Discord bot logged in as ${client.user.tag}`);
    log('INFO', 'Bot is in servers:', { 
      servers: client.guilds.cache.map(guild => ({ id: guild.id, name: guild.name }))
    });
  });

  await client.login(defaulted.BOT_TOKEN);

  const configSig = getConfigSignature(defaulted);
  let state = readJsonFile(STATE_FILE, null);
  if (!state || state.configSignature !== configSig) {
    const { schedule, effectiveEnd } = generateFullSchedule(defaulted);
    state = {
      configSignature: configSig,
      schedule,
      effectiveEnd,
      sentCount: 0,
      totalUsdRaised: defaulted.startRaisedUsd,
      completed: false
    };
    writeJsonFile(STATE_FILE, state);
    const counts = state.schedule.reduce((acc, item) => {
      const kind = typeof item === 'string' ? 'buy' : (item.kind || 'buy');
      acc[kind] = (acc[kind] || 0) + 1;
      return acc;
    }, {});
    const firstIso = state.schedule.length ? (typeof state.schedule[0] === 'string' ? state.schedule[0] : state.schedule[0].at) : null;
    const lastIso = state.schedule.length ? (typeof state.schedule[state.schedule.length - 1] === 'string' ? state.schedule[state.schedule.length - 1] : state.schedule[state.schedule.length - 1].at) : null;
    log('INFO', 'Planned schedule', { total: state.schedule.length, effectiveEnd, counts, firstAt: firstIso, lastAt: lastIso });
  }

  // Start HTTP server for status
  const server = http.createServer((req, res) => {
    try {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
      if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
      }

      if (req.method === 'GET' && req.url && req.url.startsWith('/status')) {
        const remaining = Math.max(0, (state?.schedule?.length || 0) - (state?.sentCount || 0));
        const firstPending = state && state.schedule && state.sentCount < state.schedule.length
          ? (typeof state.schedule[state.sentCount] === 'string' ? state.schedule[state.sentCount] : state.schedule[state.sentCount].at)
          : null;
        const resp = {
          totalUsdRaised: state?.totalUsdRaised ?? 0,
          sentCount: state?.sentCount ?? 0,
          remaining,
          completed: !!state?.completed,
          effectiveEnd: state?.effectiveEnd ?? null,
          nextAt: firstPending,
          countdownStartUtc: defaulted?.specialPhase?.countdownStartUtc ?? null,
          presaleStartUtc: defaulted?.specialPhase?.presaleStartUtc ?? null
        };
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(resp));
        return;
      }

      res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'Not Found' }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'Internal Server Error' }));
    }
  });
  server.listen(PUBLIC_PORT, () => {
    log('INFO', 'Public HTTP server listening', { port: PUBLIC_PORT });
  });

  const { BOT_TOKEN, TARGET_CHANNEL_ID } = defaulted;

  async function sendNextIfDue() {
    if (!state || state.completed) return;
    if (state.sentCount >= state.schedule.length) {
      state.completed = true;
      writeJsonFile(STATE_FILE, state);
      log('INFO', 'All messages sent');
      return;
    }

    const internetNow = getCurrentUtcTime();
    const nextRaw = state.schedule[state.sentCount];
    let nextIso = typeof nextRaw === 'string' ? nextRaw : nextRaw.at;
    const nextEvent = typeof nextRaw === 'string' ? { kind: 'buy', at: nextIso } : { ...nextRaw };
    let nextTs = new Date(nextIso).getTime();
    let msUntil = nextTs - internetNow.getTime();

    const MAX_ALLOWED_DELAY_MS = 30 * 1000;
    while (msUntil < -MAX_ALLOWED_DELAY_MS && state.sentCount < state.schedule.length) {
      log('WARN', 'Skipping past event (too old)', { kind: nextEvent.kind, at: nextIso, behindMs: -msUntil });
      state.sentCount += 1;
      writeJsonFile(STATE_FILE, state);
      if (state.sentCount >= state.schedule.length) {
        state.completed = true;
        writeJsonFile(STATE_FILE, state);
        log('INFO', 'All messages sent');
        return;
      }
      const nr = state.schedule[state.sentCount];
      const ni = typeof nr === 'string' ? nr : nr.at;
      const ne = typeof nr === 'string' ? { kind: 'buy', at: ni } : { ...nr };
      nextTs = new Date(ni).getTime();
      msUntil = nextTs - internetNow.getTime();
      nextIso = ni;
      nextEvent.kind = ne.kind;
      nextEvent.at = ne.at;
    }

    if (msUntil <= 0) {
      const wasLate = msUntil < 0;
      try {
        if (nextEvent.kind === 'buy') {
          const nextTotal = state.totalUsdRaised + defaulted.amountPerMessageUsd;
          const messageData = buildDiscordMessage(internetNow, nextTotal, defaulted.tokensPerHundred);
          
          const files = [];
          if (messageData.image) {
            const imagePath = path.join(ROOT_DIR, messageData.image);
            if (fs.existsSync(imagePath)) {
              files.push(new AttachmentBuilder(imagePath, { name: messageData.image }));
              messageData.embed.setImage(`attachment://${messageData.image}`);
            }
          }
          
          await sendDiscordMessage(client, TARGET_CHANNEL_ID, '', [messageData.embed], files);
          state.sentCount += 1;
          state.totalUsdRaised = nextTotal;
          const logData = { 
            progress: `${state.sentCount}/${state.schedule.length}`, 
            totalRaised: `$${formatMoney(nextTotal)}`
          };
          if (wasLate) logData.lateByMs = -msUntil;
          log('INFO', 'Buy sent', logData);
        } else if (nextEvent.kind === 'countdown' || nextEvent.kind === 'start') {
          let msg = nextEvent.text;
          const files = [];
          
          if (nextEvent.image) {
            const imagePath = path.join(ROOT_DIR, nextEvent.image);
            if (fs.existsSync(imagePath)) {
              files.push(new AttachmentBuilder(imagePath, { name: nextEvent.image }));
            }
          }
          
          if (!msg && nextEvent.kind === 'start') {
            msg = `🎯 BBLIP PRESALE IS LIVE! 🎯

⚡ First Come, First Served — Only the first 14,000 investors get in.
👥 Whitelisted: 85,312
💰 Max: $100 each

💎 Supply: 10,000,000 $BBLP
📊 Hard Cap: $1,400,000

📱 Follow: [Telegram](https://t.me/BblipProtocol_Annoucements)
🔗 Secure your spot now: [bblip.io/presale](https://www.bblip.io/presale)`;
          }
          
          await sendDiscordMessage(client, TARGET_CHANNEL_ID, msg, [], files);
          state.sentCount += 1;
          log('INFO', 'Event sent', { kind: nextEvent.kind, progress: `${state.sentCount}/${state.schedule.length}` });
        } else {
          state.sentCount += 1;
        }
        writeJsonFile(STATE_FILE, state);
      } catch (err) {
        log('ERROR', 'Send failed, will retry shortly', { error: err.message || String(err) });
      }
      setTimeout(sendNextIfDue, 1_000);
      return;
    }

    log('INFO', 'Waiting for next event', { nextKind: nextEvent.kind, at: nextIso, msUntil });
    setTimeout(sendNextIfDue, Math.min(msUntil, 5_000));
  }

  if (defaulted.enforceHardStopAtEnd) {
    const endTs = new Date(defaulted.endTimeUtc).getTime();
    state.schedule = state.schedule.filter(item => {
      const iso = typeof item === 'string' ? item : item.at;
      return new Date(iso).getTime() <= endTs;
    });
    writeJsonFile(STATE_FILE, state);
  }

  sendNextIfDue();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});