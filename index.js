/**
 * session-hub — cross-session / cross-workspace messaging + persistent team task board for DSH.
 *
 * Loaded as a plain host plugin (file:// URL) from the profile patch layer.
 * Gives every agent a small set of tools:
 *   session_list          list workspaces + sessions (with aliases and live status)
 *   session_create        create a persistent worker session in a target workspace
 *   session_send          send a message / dispatch a task (auto-wake)
 *   session_reply         reply to a received message (auto-wake back)
 *   session_report        worker reports task progress / completion
 *   session_task_status   inspect one task
 *   session_team_status   roster + task board overview
 *   session_alias         bind / unbind a friendly alias to a session
 *   session_bridge_log    recent cross-session timeline
 *
 * Design notes:
 * - Delivery uses the public ctx.sessionController.prompt() API, which resumes a cold
 *   session and wakes an idle agent (followup). When the target is running we can steer
 *   at its next step boundary (urgent messages only).
 * - Completion notification is event driven: we listen to ctx.on("session/event") and when
 *   a worker's turn ends while it owns a running task, we automatically deliver a notice
 *   back to the dispatcher, so the dispatcher never has to block and wait.
 * - Everything (aliases, roster, task board, message ledger) is persisted as JSON under
 *   $DSH_HOME/session-hub/state.json so it survives restarts.
 */

import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';

export const name = 'session-hub';
export const inject = ['tools', 'sessionController', 'workspaceRegistry', 'sessionQuery'];

const STATE_VERSION = 1;
const MAX_TEXT_BYTES = 16 * 1024;
const MAX_LEDGER = 300;
const MAX_TASKS = 300;
const DEFAULT_MAX_HOPS = 8;
const DEFAULT_RATE_PER_MINUTE = 20;

const STATUSES = ['pending', 'running', 'done', 'blocked', 'failed', 'cancelled'];

export function apply(ctx, config = {}) {
  const cfg = {
    completionNotify: true,
    maxHops: DEFAULT_MAX_HOPS,
    maxMessagesPerMinute: DEFAULT_RATE_PER_MINUTE,
    /** Compact one-line notices/reports so agent-to-agent chatter does not flood the transcript. */
    quietMode: true,
    noticeTextLimit: 160,
    /** 'chat' delivers reports/notices into the transcript; 'board' keeps them board/inbox-only. */
    deliverReports: 'chat',
    // Workspace path patterns (case-insensitive substring, or /regex/) whose task
    // dispatches require explicit user approval before delivery.
    approvalWorkspaces: [],
    approvalTimeoutSeconds: 300,
    ...(config && typeof config === 'object' ? config : {}),
  };

  const PLUGIN_DIR = path.dirname(fileURLToPath(import.meta.url));
  const stateDir = path.join(process.env.DSH_HOME || process.cwd(), 'session-hub');
  const statePath = path.join(stateDir, 'state.json');

  /** @type {{version:number, aliases:Record<string,string>, members:Record<string,{alias?:string, role?:string, workspaceId?:string, joinedAt:number}>, tasks:Record<string, any>, messages:any[], teams:Record<string, any>, seq:number}} */
  let state = loadState();
  let saveTimer = null;

  /** last assistant text per session, for completion reports */
  const lastAssistantText = new Map();
  /** when the last assistant text was captured, for bounded-memory cleanup */
  const lastAssistantAt = new Map();
  /** sliding rate window per target session */
  const rateWindows = new Map();
  /** per-worker dispatch locks: one in-flight dispatch per worker at most */
  const dispatchLocks = new Set();
  /** last known session titles, so labels never fall back to snowflake ids */
  const sessionTitleCache = new Map();
  const MAINTENANCE_INTERVAL_MS = 5 * 60_000;
  const MAX_PENDING_TASKS_PER_WORKER = 50;
  const MAX_BROADCAST_TARGETS = 50;
  const MAX_SMALL_TEXT = 200;

  // ---------------------------------------------------------------- persistence

  function loadState() {
    const empty = { version: STATE_VERSION, aliases: {}, members: {}, tasks: {}, messages: [], teams: {}, deliveries: {}, readMarks: {}, seq: 0 };
    try {
      const raw = fs.readFileSync(statePath, 'utf8');
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        return {
          version: STATE_VERSION,
          aliases: parsed.aliases && typeof parsed.aliases === 'object' ? parsed.aliases : {},
          members: parsed.members && typeof parsed.members === 'object' ? parsed.members : {},
          tasks: parsed.tasks && typeof parsed.tasks === 'object' ? parsed.tasks : {},
          messages: Array.isArray(parsed.messages) ? parsed.messages : [],
          teams: parsed.teams && typeof parsed.teams === 'object' ? parsed.teams : {},
          deliveries: parsed.deliveries && typeof parsed.deliveries === 'object' ? parsed.deliveries : {},
          readMarks: parsed.readMarks && typeof parsed.readMarks === 'object' ? parsed.readMarks : {},
          seq: typeof parsed.seq === 'number' ? parsed.seq : 0,
        };
      }
    } catch (error) {
      if (error?.code === 'ENOENT') return empty;
      try {
        const backup = `${statePath}.corrupt-${Date.now()}`;
        fs.renameSync(statePath, backup);
        ctx.logger?.warn?.(`session-hub: state.json unreadable (${String(error?.message || error)}); moved to ${backup}`);
      } catch (renameError) {
        ctx.logger?.warn?.(`session-hub: state.json unreadable and could not be backed up: ${String(renameError?.message || renameError)}`);
      }
    }
    return empty;
  }

  function saveStateNow() {
    try {
      fs.mkdirSync(stateDir, { recursive: true });
      const tmp = `${statePath}.${process.pid}.tmp`;
      fs.writeFileSync(tmp, JSON.stringify(state, null, 2), 'utf8');
      fs.renameSync(tmp, statePath);
    } catch (error) {
      ctx.logger?.warn?.(`session-hub: failed to persist state: ${String(error)}`);
    }
  }

  function saveStateSoon() {
    if (saveTimer) return;
    saveTimer = setTimeout(() => {
      saveTimer = null;
      saveStateNow();
    }, 250);
    saveTimer.unref?.();
  }

  // Flush pending state before the plugin instance goes away, so a reload can never
  // race an old delayed write against the next instance.
  ctx.effect(() => () => {
    if (saveTimer) {
      clearTimeout(saveTimer);
      saveTimer = null;
    }
    saveStateNow();
  }, 'session-hub.save-timer');

  // Periodic housekeeping: bounded maps, expired approvals.
  ctx.effect(() => {
    const timer = setInterval(() => {
      const now = Date.now();
      for (const [key, window] of rateWindows) {
        const kept = window.filter((ts) => now - ts < 60_000);
        if (kept.length > 0) rateWindows.set(key, kept);
        else rateWindows.delete(key);
      }
      for (const [sessionId, at] of lastAssistantAt) {
        if (now - Number(at || 0) > 10 * 60_000) lastAssistantAt.delete(sessionId);
      }
      for (const sessionId of [...lastAssistantText.keys()]) {
        const busy = Object.values(state.tasks).some((task) => task.to === sessionId
          && (task.status === 'running' || task.status === 'sending' || task.status === 'queued'));
        if (!busy && !state.members[sessionId] && !lastAssistantAt.has(sessionId)) lastAssistantText.delete(sessionId);
      }
      for (const [sessionId, list] of Object.entries(state.deliveries || {})) {
        const kept = (list || []).filter((entry) => now - Number(entry.at || 0) < 24 * 60 * 60_000);
        if (kept.length > 0) state.deliveries[sessionId] = kept;
        else delete state.deliveries[sessionId];
      }
      const approvalTtl = Math.max(600_000, Number(approvalPolicy().timeoutSeconds || 300) * 2 * 1000);
      for (const task of Object.values(state.tasks)) {
        if (task.status !== 'awaiting-approval') continue;
        const askedAt = Number(task.approval?.askedAt || task.createdAt || 0);
        if (askedAt > 0 && now - askedAt > approvalTtl) {
          updateTask(task.taskId, { status: 'cancelled', result: 'approval timed out' });
        }
      }
    }, MAINTENANCE_INTERVAL_MS);
    timer.unref?.();
    return () => clearInterval(timer);
  }, 'session-hub.maintenance');

  function nextSeq() {
    state.seq += 1;
    return state.seq;
  }

  // ---------------------------------------------------------------- helpers

  function assertText(value, label) {
    if (typeof value !== 'string' || value.trim().length === 0) throw new Error(`${label} must be a non-empty string`);
    if (Buffer.byteLength(value, 'utf8') > MAX_TEXT_BYTES) throw new Error(`${label} exceeds ${MAX_TEXT_BYTES} bytes`);
    return value;
  }

  function short(messageId) {
    return typeof messageId === 'string' ? messageId.slice(0, 8) : String(messageId);
  }

  function sessionLabel(sessionId) {
    const alias = Object.entries(state.aliases).find(([, id]) => id === sessionId)?.[0];
    const member = state.members[sessionId];
    if (alias) return alias;
    if (member?.alias) return member.alias;
    const cached = sessionTitleCache.get(sessionId);
    if (cached) return cached;
    const raw = String(sessionId || '');
    return raw.startsWith('session-') ? `会话 ${raw.slice(8, 16)}` : (raw || '未知会话');
  }

  function callerSessionId(exec) {
    const id = exec?.agent?.session?.header?.id || exec?.agent?.id;
    if (!id) throw new Error('this tool requires a calling agent session');
    return id;
  }

  async function listSessionSummaries(signal) {
    const listed = await ctx.sessionController.list(undefined, signal);
    const summaries = Array.isArray(listed) ? listed : (Array.isArray(listed?.items) ? listed.items : []);
    const out = [];
    for (const summary of summaries) {
      let title;
      try {
        const snapshot = await ctx.sessionQuery.readTitle(summary.sessionId, signal);
        title = typeof snapshot === 'string' ? snapshot
          : (snapshot && typeof snapshot.title === 'string' ? snapshot.title : undefined);
      } catch (error) {
        ctx.logger?.info?.(`session-hub: could not read session title: ${safeErrorText(error)}`);
        title = undefined;
      }
      if (title) sessionTitleCache.set(summary.sessionId, title);
      out.push({
        sessionId: summary.sessionId,
        title: title || undefined,
        cwd: summary.cwd,
        running: summary.running === true,
        agentAvailable: summary.agentAvailable === true,
        updatedAt: summary.updatedAt,
        blank: summary.blank === true,
        alias: Object.entries(state.aliases).find(([, id]) => id === summary.sessionId)?.[0],
      });
    }
    out.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
    return out;
  }

  function normalizeDir(input) {
    return path.resolve(String(input)).replace(/[\\/]+$/, '').toLowerCase();
  }

  function listWorkspaces() {
    return ctx.workspaceRegistry.list().map((ws) => ({ id: ws.id, title: ws.title, path: ws.path, sessionIds: ws.sessionIds }));
  }

  async function resolveTarget(target, signal) {
    const raw = assertText(String(target ?? ''), 'target');
    if (state.aliases[raw]) return { sessionId: state.aliases[raw], via: 'alias' };
    const memberByAlias = Object.entries(state.members).find(([, member]) => member.alias === raw);
    if (memberByAlias) return { sessionId: memberByAlias[0], via: 'member' };

    const sessions = await listSessionSummaries(signal);
    const byId = sessions.find((s) => s.sessionId === raw);
    if (byId) return { sessionId: byId.sessionId, via: 'id' };

    const lowered = raw.toLowerCase();
    const matches = sessions.filter((s) => (s.title || '').toLowerCase().includes(lowered)
      || s.sessionId.toLowerCase().startsWith(lowered));
    if (matches.length === 1) return { sessionId: matches[0].sessionId, via: 'title' };
    if (matches.length > 1) {
      throw new Error(`target "${String(raw).slice(0, 80)}" matched ${matches.length} sessions: ${matches.slice(0, 5).map((m) => `${sessionLabel(m.sessionId)}(${m.sessionId.slice(0, 8)}…)`).join(', ')} — use an alias or exact id`);
    }
    throw new Error(`target session "${raw}" not found: use session_list, an exact session id, or an alias`);
  }

  function checkRate(sessionId) {
    const now = Date.now();
    const window = rateWindows.get(sessionId) || [];
    const kept = window.filter((ts) => now - ts < 60_000);
    if (kept.length >= cfg.maxMessagesPerMinute) {
      throw new Error(`rate limit: more than ${cfg.maxMessagesPerMinute} messages/minute to ${sessionLabel(sessionId)}`);
    }
    kept.push(now);
    rateWindows.set(sessionId, kept);
  }

  /**
   * Deliver one agent-authored message into a target session.
   * - target idle   -> followup wakes it immediately
   * - target running-> followup queues to its next turn (or steer if urgent)
   */
  async function deliver(targetSessionId, text, options = {}) {
    checkRate(targetSessionId);
    let agent;
    try {
      agent = await ctx.sessionController.resolveAgent(targetSessionId);
    } catch (error) {
      throw new Error(`cannot reach session ${sessionLabel(targetSessionId)}: ${safeErrorText(error)}`);
    }
    const running = agent?.status === 'running';
    const mode = running && options.urgent === true ? 'steer' : 'followup';
    const signal = options.signal ?? new AbortController().signal;
    await ctx.sessionController.prompt({
      sessionId: targetSessionId,
      requestId: `session-hub-${randomUUID()}`,
      content: [{ type: 'text', text }],
      ...(mode === 'steer' ? { mode: 'steer' } : {}),
    }, signal);
    return { mode, running };
  }

  function envelope({ kind, messageId, taskId, from, fromLabel, toLabel, text, hops }) {
    if (cfg.quietMode) {
      return `${fromLabel || from}：${text}`;
    }
    const lines = [
      '【会话间消息 / agent-to-agent message】',
      `kind: ${kind}`,
      `messageId: ${messageId}`,
      taskId ? `taskId: ${taskId}` : undefined,
      `from: ${fromLabel} (${from})`,
      `to: ${toLabel}`,
      `hops: ${hops}`,
      '---',
      text,
    ].filter(Boolean);
    return lines.join('\n');
  }

  function guidance({ kind, messageId, taskId }) {
    if (cfg.quietMode) {
      if (kind === 'task') return '\n[要求] 完成/受阻后调用 session_report({status:"done"|"blocked"|"failed",summary:"..."})（自动对应你最近的任务）；需对方决策时用 session_reply({message:"..."})；无关内容不要回复。';
      if (kind === 'question') return '\n[要求] 用 session_reply({message:"..."}) 回答对方。';
      if (kind === 'result') return '\n（结果通知，无需回复。）';
      return '';
    }
    if (kind === 'task') {
      return [
        '',
        '[处理要求] 完成任务后调用 session_report({taskId, status:"done"|"blocked"|"failed", summary:"..."})。',
        '需要对方决策时调用 session_reply({messageId, message:"..."})；无关内容不要回复，避免来回刷屏。',
      ].join('\n');
    }
    if (kind === 'question') {
      return `\n[处理要求] 用 session_reply({messageId:"${messageId}", message:"..."}) 回答；这是对方在等你结论。`;
    }
    if (kind === 'result') return '\n（这是结果/通知消息，无需回复。）';
    return '';
  }

  function recordMessage(entry) {
    const record = typeof entry.text === 'string' ? { ...entry, text: trimText(entry.text, 600) } : entry;
    if (entry.to && entry.messageId) {
      const list = Array.isArray(state.deliveries[entry.to]) ? state.deliveries[entry.to] : [];
      list.push({
        messageId: entry.messageId,
        ...(entry.taskId ? { taskId: entry.taskId } : {}),
        kind: entry.kind,
        from: entry.from,
        at: Date.now(),
      });
      state.deliveries[entry.to] = list.slice(-20);
    }
    state.messages.push({ ...record, at: Date.now() });
    if (state.messages.length > MAX_LEDGER) state.messages = state.messages.slice(-MAX_LEDGER);
    saveStateSoon();
  }

  function memberEntry(sessionId, patch = {}) {
    const existing = state.members[sessionId] || { joinedAt: Date.now() };
    state.members[sessionId] = { ...existing, ...patch };
    if (!state.members[sessionId].title) {
      const known = patch.title || sessionTitleCache.get(sessionId);
      if (known) state.members[sessionId].title = known;
    }
    saveStateSoon();
    return state.members[sessionId];
  }

  function updateTask(taskId, patch) {
    const task = state.tasks[taskId];
    if (!task) return undefined;
    Object.assign(task, patch, { updatedAt: Date.now() });
    saveStateSoon();
    return task;
  }

  function trimText(text, max = 1800) {
    const value = String(text ?? '').trim();
    if (value.length <= max) return value;
    return `${value.slice(0, max)}…（截断，共 ${value.length} 字符）`;
  }

  /** Redact absolute filesystem paths from error text before it reaches the model. */
  function safeErrorText(error) {
    const text = String(error?.message || error);
    return text.replace(/[A-Za-z]:\\[^\s'"`]+/g, '<path>').replace(/\\\\[^\s'"`]+/g, '<path>');
  }

  function assertShort(value, label, max = MAX_SMALL_TEXT) {
    const text = String(value ?? '');
    if (text.length > max) throw new Error(`${label} is too long (max ${max} characters)`);
    return text;
  }

  /** Resolve an export path, refusing anything outside the session-hub data directory. */
  function resolveExportTarget(input) {
    const root = path.resolve(stateDir);
    const fallback = path.join(root, `timeline-${new Date().toISOString().replace(/[:.]/g, '-')}.md`);
    if (typeof input !== 'string' || input.trim() === '') return fallback;
    const raw = input.trim();
    if (raw.length > 500) throw new Error('export path is too long (max 500 characters)');
    const resolved = path.isAbsolute(raw) ? path.resolve(raw) : path.resolve(root, raw);
    if (resolved !== root && !resolved.startsWith(root + path.sep)) {
      throw new Error('export path must be inside the session-hub data directory');
    }
    return resolved;
  }

  /** The one task currently occupying a worker (running or being delivered). */
  function activeTaskFor(workerId) {
    return Object.values(state.tasks).find((task) => task.to === workerId
      && (task.status === 'running' || task.status === 'sending'));
  }

  /** Oldest queued task for one worker, if any. */
  function nextQueuedTask(workerId) {
    return Object.values(state.tasks)
      .filter((task) => task.to === workerId && task.status === 'queued')
      .sort((a, b) => a.createdAt - b.createdAt)[0];
  }

  /** Deliver one task message to its worker and move it to running. */
  async function dispatchTaskUnlocked(task) {
    // Reset the captured assistant text so a completion notice for this task can
    // never reuse the previous task's output (queued workers run several tasks).
    lastAssistantText.delete(task.to);
    const messageId = `msg-${randomUUID()}`;
    const text = [
      envelope({ kind: 'task', messageId, taskId: task.taskId, from: task.from, fromLabel: sessionLabel(task.from), toLabel: sessionLabel(task.to), text: task.body || task.title || '', hops: task.hops || 1 }),
      guidance({ kind: 'task', messageId, taskId: task.taskId }),
    ].join('\n');
    updateTask(task.taskId, { status: 'sending', lastMessageId: messageId });
    try {
      const delivered = await deliver(task.to, text, { urgent: task.urgent === true });
      updateTask(task.taskId, { status: 'running', dispatchedAt: Date.now(), lastMessageId: messageId });
      recordMessage({ messageId, taskId: task.taskId, from: task.from, to: task.to, kind: 'task', hops: task.hops || 1, status: 'delivered', mode: delivered.mode, text: task.body || task.title || '' });
      return { ok: true, messageId, mode: delivered.mode };
    } catch (error) {
      updateTask(task.taskId, { status: 'failed', result: `delivery failed: ${safeErrorText(error)}` });
      return { ok: false, error: safeErrorText(error) };
    }
  }

  /** Serial per-worker dispatch gate: concurrent callers get busy instead of double dispatch. */
  async function dispatchTask(task) {
    if (dispatchLocks.has(task.to)) return { ok: false, busy: true };
    dispatchLocks.add(task.to);
    try {
      return await dispatchTaskUnlocked(task);
    } finally {
      dispatchLocks.delete(task.to);
    }
  }

  /** Strict serial queue: dispatch the next queued task once the worker is free. */
  async function drainQueue(workerId) {
    if (activeTaskFor(workerId)) return undefined;
    const next = nextQueuedTask(workerId);
    if (!next) return undefined;
    ctx.logger?.info?.(`session-hub: dispatching queued task ${next.taskId} to ${workerId}`);
    const result = await dispatchTask(next);
    return result?.busy ? undefined : result;
  }

  const policyPath = path.join(stateDir, 'policy.json');
  let policyCache = { mtime: 0, value: null };

  /** Effective approval policy: policy.json (hot reloaded) wins over patch config. */
  function approvalPolicy() {
    let fileValue = null;
    try {
      const stat = fs.statSync(policyPath);
      if (stat.mtimeMs !== policyCache.mtime) {
        policyCache = { mtime: stat.mtimeMs, value: JSON.parse(fs.readFileSync(policyPath, 'utf8')) };
      }
      fileValue = policyCache.value;
    } catch (error) {
      ctx.logger?.info?.(`session-hub: approval policy unreadable, using patch config: ${safeErrorText(error)}`);
      fileValue = null;
    }
    const patterns = fileValue?.approvalWorkspaces ?? cfg.approvalWorkspaces;
    const timeoutSeconds = fileValue?.approvalTimeoutSeconds ?? cfg.approvalTimeoutSeconds;
    return { workspaces: Array.isArray(patterns) ? patterns : [], timeoutSeconds };
  }

  /** Whether one workspace path matches the approval policy. */
  function approvalRequiredFor(cwd) {
    const value = String(cwd || '');
    if (!value) return false;
    const policy = approvalPolicy();
    return policy.workspaces.some((pattern) => {
      if (typeof pattern !== 'string' || pattern.length === 0) return false;
      if (pattern.length > 2 && pattern.startsWith('/') && pattern.endsWith('/')) {
        try {
          return new RegExp(pattern.slice(1, -1), 'i').test(value);
        } catch (error) {
          ctx.logger?.warn?.(`session-hub: invalid approval regex pattern ignored: ${safeErrorText(error)}`);
          return false;
        }
      }
      return value.toLowerCase().includes(pattern.toLowerCase());
    });
  }

  /** Ask the human to approve one task dispatch. Returns approved / rejected / pending. */
  /** Resolve each session's workspace path (used by the approval policy). */
  async function sessionCwdIndex(signal) {
    const index = new Map();
    try {
      const summaries = await listSessionSummaries(signal);
      for (const summary of summaries) index.set(summary.sessionId, summary.cwd || '');
    } catch (error) {
      ctx.logger?.info?.(`session-hub: could not resolve workspaces for approval check: ${safeErrorText(error)}`);
    }
    return index;
  }

  async function askDispatchApproval(exec, request) {
    const userQuestions = ctx.get?.('userQuestions');
    if (!userQuestions || typeof userQuestions.askTimed !== 'function' || !exec?.callId || !exec?.agent) {
      return { approved: false, pending: true, reason: 'approval unavailable: no user-questions answerer or tool call id' };
    }
    const what = request.kind === 'task' ? '任务' : '消息';
    const question = {
      id: 'approve-dispatch',
      header: '跨会话任务审批',
      detail: request.cwd,
      question: `是否允许把${what}发送到工作区「${request.cwd}」的 ${request.targetLabel}？\n\n${request.kind === 'task' ? `任务：${request.title}\n` : ''}${String(request.body || '').slice(0, 600)}`,
      options: [{ label: '批准派发' }, { label: '拒绝' }],
    };
    const policyTimeout = Number(approvalPolicy().timeoutSeconds);
    const timeoutSeconds = Number.isFinite(policyTimeout) && policyTimeout > 0
      ? Math.min(policyTimeout, 3600)
      : 300;
    try {
      const result = await userQuestions.askTimed({
        questions: [question],
        agent: exec.agent,
        ...(exec.signal ? { signal: exec.signal } : {}),
      }, exec.callId, Math.round(timeoutSeconds * 1000));
      const answer = Array.isArray(result?.answers) ? result.answers.find((item) => item.id === 'approve-dispatch') : undefined;
      const selected = Array.isArray(answer?.selected) ? answer.selected : [];
      if (selected.includes('批准派发')) return { approved: true };
      if (selected.includes('拒绝')) return { approved: false, rejected: true, reason: '用户拒绝了这次派发' };
      return { approved: false, pending: true, reason: 'approval pending (no answer yet)' };
    } catch (error) {
      return { approved: false, pending: true, reason: `approval pending: ${safeErrorText(error)}` };
    }
  }

  // ---------------------------------------------------------------- completion notifications

  ctx.on('session/event', (session, event) => {
    if (!session || !event) return;
    const sessionId = session.header?.id || session.id;
    if (!sessionId) return;
    if (event.type === 'assistant/message') {
      try {
        const content = event.data?.message?.content;
        if (Array.isArray(content)) {
          const text = content.filter((block) => block?.type === 'text' && typeof block.text === 'string').map((block) => block.text).join('\n').trim();
          if (text) {
            lastAssistantText.set(sessionId, trimText(text, 4000));
            lastAssistantAt.set(sessionId, Date.now());
          }
        }
      } catch (error) {
        ctx.logger?.info?.(`session-hub: ignored malformed session event: ${safeErrorText(error)}`);
      }
      return;
    }
    if (event.type === 'turn/end') {
      const openTasks = Object.values(state.tasks).filter((task) => task.to === sessionId && task.status === 'running');
      if (openTasks.length === 0) return;
      const reasonKind = event.data?.reason?.kind || 'completed';
      const status = reasonKind === 'completed' || reasonKind === 'max-tokens' ? 'done'
        : reasonKind === 'aborted' ? 'blocked' : 'failed';
      for (const task of openTasks) {
        if (task.notifiedAt) continue;
        updateTask(task.taskId, { status, stopReason: reasonKind, result: lastAssistantText.get(sessionId) || undefined, notifiedAt: Date.now() });
        const compactText = trimText(lastAssistantText.get(sessionId) || '', cfg.noticeTextLimit);
        const note = cfg.quietMode
          ? [
              `【任务完成】${status === 'done' ? '✓ 完成' : status === 'blocked' ? '⏸ 阻塞' : '✗ 失败'} · ${task.title || task.taskId}（${sessionLabel(sessionId)}）`,
              status === 'done' ? (compactText ? trimText(compactText, 120) : '') : compactText,
              '（自动通知，详情见任务板；无需回复。）',
            ].filter(Boolean).join('\n')
          : [
              '【任务自动完成通知 / automatic task notice】',
              `taskId: ${task.taskId}`,
              `worker: ${sessionLabel(sessionId)} (${sessionId})`,
              `status: ${status}`,
              `stopReason: ${reasonKind}`,
              task.title ? `title: ${task.title}` : undefined,
              '---',
              lastAssistantText.get(sessionId) || '（没有捕获到 assistant 文本）',
              '',
              '（自动通知，无需回复。需要继续追问请用 session_send。）',
            ].filter(Boolean).join('\n');
        recordMessage({ messageId: `auto-${randomUUID()}`, taskId: task.taskId, from: sessionId, to: task.from, kind: 'result', hops: (task.hops || 1) + 1, status: 'delivered', auto: true, text: lastAssistantText.get(sessionId) || '（没有捕获到 assistant 输出）' });
        if (cfg.completionNotify && cfg.deliverReports !== 'board') {
          void deliver(task.from, note, { urgent: false }).catch((error) => {
            ctx.logger?.warn?.(`session-hub: completion notice for task ${task.taskId} failed: ${String(error?.message || error)}`);
          });
        }
      }
      void drainQueue(sessionId).catch((error) => {
        ctx.logger?.warn?.(`session-hub: draining queued tasks for ${sessionId} failed: ${String(error?.message || error)}`);
      });
    }
  });

  // ---------------------------------------------------------------- tools

  const T = {
    session_list: {
      name: 'session_list',
      description: 'List DSH workspaces and sessions (id, title, cwd, running, alias). Use it to find the target session or workspace before session_send / session_create. This is a read-only lookup and does not wake anything.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          query: { type: 'string', description: 'Optional case-insensitive filter over workspace title/path, session title, alias or id.' },
          limit: { type: 'integer', description: 'Max sessions to return (default 30).' },
        },
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const limit = Math.min(Math.max(Number(args.limit) || 30, 1), 100);
        const query = typeof args.query === 'string' && args.query.trim()
          ? assertShort(args.query.trim(), 'query').toLowerCase()
          : undefined;
        const workspaces = listWorkspaces().filter((ws) => !query
          || ws.title.toLowerCase().includes(query) || ws.path.toLowerCase().includes(query));
        const sessions = (await listSessionSummaries(exec?.signal)).filter((s) => !query
          || (s.title || '').toLowerCase().includes(query)
          || s.sessionId.toLowerCase().includes(query)
          || (s.alias || '').toLowerCase().includes(query)
          || (s.cwd || '').toLowerCase().includes(query)).slice(0, limit);
        const lines = [];
        lines.push(`Workspaces (${workspaces.length}):`);
        for (const ws of workspaces.slice(0, 20)) lines.push(`- ${ws.title} | ${ws.path} | id=${ws.id} | sessions=${ws.sessionIds.length}`);
        lines.push('');
        lines.push(`Sessions (${sessions.length}):`);
        for (const s of sessions) {
          lines.push(`- ${s.alias ? `@${s.alias}` : ''}${s.title || '(untitled)'} | ${s.sessionId} | ${s.running ? 'running' : 'idle'} | ${s.cwd || '-'}`);
        }
        return { text: lines.join('\n'), workspaces, sessions };
      },
    },

    session_create: {
      name: 'session_create',
      description: 'Create a persistent worker session in a target workspace (registering the directory as a workspace when needed). The new session inherits the caller\'s preset and model by default. Optionally set a title, an alias and an initial task message. Returns the new session id.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          workspace: { type: 'string', description: 'Workspace id, exact title, or absolute directory path. A path that is not registered yet is registered as a new workspace.' },
          path: { type: 'string', description: 'Absolute directory path when you prefer to pass it explicitly instead of workspace.' },
          title: { type: 'string', description: 'Session title shown in the sidebar.' },
          alias: { type: 'string', description: 'Short alias other tools can use instead of the session id, e.g. ump-dev.' },
          firstMessage: { type: 'string', description: 'Optional message to deliver right away (the worker starts working immediately).' },
          urgent: { type: 'boolean', description: 'When firstMessage is given, steer at the worker\'s next step instead of queueing a new turn.' },
        },
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const caller = callerSessionId(exec);
        const callerAgent = exec?.agent;
        const targetInput = typeof args.workspace === 'string' && args.workspace.trim()
          ? args.workspace.trim()
          : (typeof args.path === 'string' && args.path.trim() ? args.path.trim() : undefined);
        if (!targetInput) throw new Error('session_create needs `workspace` (id/title/path) or `path`');

        const workspaces = ctx.workspaceRegistry.list();
        let workspace = workspaces.find((ws) => ws.id === targetInput);
        if (!workspace) workspace = workspaces.find((ws) => ws.title === targetInput);
        if (!workspace && path.isAbsolute(targetInput)) {
          const normalized = normalizeDir(targetInput);
          workspace = workspaces.find((ws) => normalizeDir(ws.path) === normalized);
        }
        let workspaceId;
        let workspaceTitle;
        if (workspace) {
          workspaceId = workspace.id;
          workspaceTitle = workspace.title;
        } else {
          if (!path.isAbsolute(targetInput)) {
            throw new Error(`workspace "${targetInput}" not found and it is not an absolute path; use session_list to see available workspaces`);
          }
          const created = await ctx.workspaceRegistry.create(targetInput);
          workspaceId = created.id;
          workspaceTitle = created.title;
        }

        const preset = callerAgent?.session?.header?.agentPreset;
        const options = callerAgent?.options || {};
        const created = await ctx.sessionController.create({
          workspaceId,
          ...(preset ? { agentPreset: preset } : {}),
        });
        const sessionId = created.sessionId;

        if (options.provider && options.model) {
          try {
            await ctx.sessionController.selectModel({
              sessionId,
              provider: options.provider,
              model: options.model,
              ...(options.reasoningEffort ? { reasoningEffort: options.reasoningEffort } : {}),
            });
          } catch (error) {
            ctx.logger?.warn?.(`session-hub: could not inherit model for ${sessionId}: ${String(error?.message || error)}`);
          }
        }
        assertShort(args.title ?? '', 'title');
        if (typeof args.title === 'string' && args.title.trim()) {
          try {
            await ctx.sessionController.rename({ sessionId, title: args.title.trim() });
          } catch (error) {
            ctx.logger?.warn?.(`session-hub: could not rename ${sessionId}: ${String(error?.message || error)}`);
          }
        }
        const alias = typeof args.alias === 'string' && args.alias.trim() ? args.alias.trim() : undefined;
        if (alias) {
          state.aliases[alias] = sessionId;
          memberEntry(sessionId, { alias, workspaceId });
        } else {
          memberEntry(sessionId, { workspaceId });
        }
        saveStateSoon();

        let delivered;
        if (typeof args.firstMessage === 'string' && args.firstMessage.trim()) {
          const messageId = `msg-${randomUUID()}`;
          const text = [
            envelope({ kind: 'task', messageId, from: caller, fromLabel: sessionLabel(caller), toLabel: sessionLabel(sessionId), text: args.firstMessage.trim(), hops: 1 }),
            guidance({ kind: 'task', messageId }),
          ].join('\n');
          delivered = await deliver(sessionId, text, { urgent: args.urgent === true, signal: exec?.signal });
          recordMessage({ messageId, from: caller, to: sessionId, kind: 'task', hops: 1, status: 'delivered', mode: delivered.mode, text: body });
        }

        const lines = [
          `created session ${sessionId}`,
          `workspace: ${workspaceTitle} (${workspaceId})`,
          preset ? `preset: ${preset}` : 'preset: default',
          options.provider && options.model ? `model: ${options.provider}/${options.model}` : 'model: default',
          alias ? `alias: @${alias}` : undefined,
          delivered ? `first message delivered (${delivered.mode})` : undefined,
        ].filter(Boolean);
        return { text: lines.join('\n'), sessionId, workspaceId, alias };
      },
    },

    session_send: {
      name: 'session_send',
      description: 'Send a message to another session (any workspace). An idle target is woken and starts a new turn; a running target queues it for its next turn, or receives it at its next step when urgent=true. kind="task" creates a tracked task on the task board, and the worker\'s completion is reported back automatically.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          target: { type: 'string', description: 'Target session id, alias, or unique title substring.' },
          message: { type: 'string', description: 'The message body.' },
          kind: { type: 'string', enum: ['task', 'question', 'note', 'result'], description: 'Message kind (default task).' },
          taskTitle: { type: 'string', description: 'Short task title for the task board (defaults to the message head).' },
          urgent: { type: 'boolean', description: 'Deliver at the target\'s next step (steer) instead of waiting for its next turn.' },
        },
        required: ['target', 'message'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const caller = callerSessionId(exec);
        const target = await resolveTarget(args.target, exec?.signal);
        if (target.sessionId === caller) throw new Error('a session cannot send a message to itself');
        const body = assertText(args.message, 'message');
        const kind = ['task', 'question', 'note', 'result'].includes(args.kind) ? args.kind : 'task';
        const hops = 1;
        assertShort(args.taskTitle ?? '', 'taskTitle');
        const cwdIndex = await sessionCwdIndex(exec?.signal);
        const targetCwd = cwdIndex.get(target.sessionId) || '';
        const approvalNeeded = approvalRequiredFor(targetCwd);
        if (approvalNeeded) {
          const decision = await askDispatchApproval(exec, {
            kind,
            cwd: targetCwd,
            targetLabel: sessionLabel(target.sessionId),
            title: (typeof args.taskTitle === 'string' && args.taskTitle.trim()) || body.slice(0, 60),
            body,
          });
          if (!decision.approved) {
            const status = decision.rejected ? 'rejected' : 'awaiting-approval';
            if (kind === 'task') {
              const gatedId = `task-${randomUUID()}`;
              state.tasks[gatedId] = {
                taskId: gatedId,
                title: (typeof args.taskTitle === 'string' && args.taskTitle.trim()) || body.slice(0, 80),
                status,
                from: caller,
                to: target.sessionId,
                hops,
                body,
                urgent: args.urgent === true,
                approval: { required: true, cwd: targetCwd, askedAt: Date.now() },
                ...(decision.reason ? { result: decision.reason } : {}),
                createdAt: Date.now(),
                updatedAt: Date.now(),
              };
              memberEntry(target.sessionId, {});
              saveStateNow();
              return status === 'rejected'
                ? { text: `task ${gatedId} rejected by the user: ${decision.reason}`, taskId: gatedId, rejected: true }
                : { text: `task ${gatedId} is awaiting user approval for workspace "${targetCwd}". Approve it in the UI or call session_task_approve({taskId:"${gatedId}"}).`, taskId: gatedId, awaitingApproval: true };
            }
            saveStateNow();
            return status === 'rejected'
              ? { text: `message not sent: the user rejected it for workspace "${targetCwd}"`, rejected: true }
              : { text: `message not sent: awaiting user approval for workspace "${targetCwd}"`, awaitingApproval: true };
          }
        }
        if (kind === 'task') {
          const pending = Object.values(state.tasks).filter((task) => task.to === target.sessionId
            && (task.status === 'queued' || task.status === 'sending' || task.status === 'running')).length;
          if (pending >= MAX_PENDING_TASKS_PER_WORKER) {
            throw new Error(`worker ${sessionLabel(target.sessionId)} already has ${pending} pending tasks (max ${MAX_PENDING_TASKS_PER_WORKER}); let it drain first`);
          }
          const taskId = `task-${randomUUID()}`;
          state.tasks[taskId] = {
            taskId,
            title: (typeof args.taskTitle === 'string' && args.taskTitle.trim()) || body.slice(0, 80),
            status: 'queued',
            from: caller,
            to: target.sessionId,
            hops,
            body,
            urgent: args.urgent === true,
            ...(approvalNeeded ? { approval: { required: true, approvedAt: Date.now(), cwd: targetCwd } } : {}),
            createdAt: Date.now(),
            updatedAt: Date.now(),
          };
          memberEntry(target.sessionId, {});
          const all = Object.values(state.tasks).sort((a, b) => b.createdAt - a.createdAt);
          if (all.length > MAX_TASKS) {
            for (const stale of all.slice(MAX_TASKS)) {
              if (!['running', 'sending', 'queued'].includes(stale.status)) delete state.tasks[stale.taskId];
            }
          }
          // The approval policy was already evaluated once, before the task was created.
          const active = activeTaskFor(target.sessionId);
          if (active) {
            saveStateSoon();
            return {
              text: `task ${taskId} queued behind ${active.taskId} — ${sessionLabel(target.sessionId)} is busy; it will be dispatched automatically when the worker's current turn ends`,
              taskId,
              queued: true,
            };
          }
          const dispatched = await dispatchTask(state.tasks[taskId]);
          if (dispatched.busy) {
            saveStateSoon();
            return { text: `task ${taskId} queued — ${sessionLabel(target.sessionId)} just started another dispatch`, taskId, queued: true };
          }
          if (!dispatched.ok) throw new Error(dispatched.error);
          return {
            text: `task ${taskId} dispatched to ${sessionLabel(target.sessionId)} (${dispatched.mode})`,
            taskId,
            messageId: dispatched.messageId,
            mode: dispatched.mode,
          };
        }
        const messageId = `msg-${randomUUID()}`;
        const text = [
          envelope({ kind, messageId, from: caller, fromLabel: sessionLabel(caller), toLabel: sessionLabel(target.sessionId), text: body, hops }),
          guidance({ kind, messageId }),
        ].join('\n');
        const delivered = await deliver(target.sessionId, text, { urgent: args.urgent === true, signal: exec?.signal });
        recordMessage({ messageId, from: caller, to: target.sessionId, kind, hops, status: 'delivered', mode: delivered.mode, text: body });
        return {
          text: `message ${short(messageId)} delivered to ${sessionLabel(target.sessionId)} (${delivered.mode}${delivered.running ? ', target running' : ', target idle → woken'})`,
          messageId,
          mode: delivered.mode,
        };
      },
    },

    session_reply: {
      name: 'session_reply',
      description: 'Reply to a cross-session message you received. Use the messageId from that message. The sender session is woken automatically if idle. Use it for questions/decisions, or to report partial results.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          messageId: { type: 'string', description: 'The messageId from the received cross-session message.' },
          message: { type: 'string', description: 'Reply body.' },
          status: { type: 'string', enum: ['result', 'blocked', 'failed'], description: 'Optional: also update the linked task status (result→done, blocked→blocked, failed→failed).' },
        },
        required: ['message'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const caller = callerSessionId(exec);
        const explicitId = typeof args.messageId === 'string' ? args.messageId.trim() : '';
        const deliveredMessageId = explicitId ? undefined
          : [...(state.deliveries[caller] || [])].reverse().find((entry) => entry.messageId && entry.kind !== 'result' && !entry.repliedAt)?.messageId;
        const source = explicitId
          ? state.messages.find((entry) => entry.messageId === explicitId)
          : (deliveredMessageId ? state.messages.find((entry) => entry.messageId === deliveredMessageId) : undefined)
            ?? [...state.messages].reverse().find((entry) => entry.to === caller && entry.kind !== 'result' && !entry.repliedAt);
        if (!source) throw new Error('no unanswered inbound message found; pass messageId or use session_send instead');
        if (source.to !== caller) throw new Error('that message was not addressed to this session');
        const messageId = source.messageId;
        const hops = (Number(source.hops) || 1) + 1;
        if (hops > cfg.maxHops) throw new Error(`reply chain too deep (${hops} > ${cfg.maxHops}); ask the user to arbitrate`);
        const body = assertText(args.message, 'message');
        const replyId = `msg-${randomUUID()}`;
        const text = [
          envelope({ kind: 'result', messageId: replyId, taskId: source.taskId, from: caller, fromLabel: sessionLabel(caller), toLabel: sessionLabel(source.from), text: body, hops }),
          guidance({ kind: 'result', messageId: replyId }),
        ].join('\n');
        const delivered = await deliver(source.from, text, { urgent: false, signal: exec?.signal });
        source.repliedAt = Date.now();
        source.status = 'replied';
        if (source.taskId && ['result', 'blocked', 'failed'].includes(args.status)) {
          const mapped = args.status === 'result' ? 'done' : args.status;
          updateTask(source.taskId, { status: mapped, result: body.slice(0, 2000), reportedAt: Date.now() });
        }
        recordMessage({ messageId: replyId, taskId: source.taskId, from: caller, to: source.from, kind: 'result', hops, status: 'delivered', mode: delivered.mode, text: body });
        return { text: `reply delivered to ${sessionLabel(source.from)} (${delivered.mode})`, messageId: replyId };
      },
    },

    session_report: {
      name: 'session_report',
      description: 'Report progress for a task you received (usually the final report). Updates the persistent task board and notifies the dispatcher. Use status done/blocked/failed and a short summary. Automatic completion notices also exist, so this is for explicit reporting or when blocked.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          taskId: { type: 'string', description: 'The taskId from the task message.' },
          status: { type: 'string', enum: ['done', 'blocked', 'failed'], description: 'Task status.' },
          summary: { type: 'string', description: 'Short summary / result / blocker.' },
        },
        required: ['status', 'summary'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const caller = callerSessionId(exec);
        const explicitTaskId = typeof args.taskId === 'string' ? args.taskId.trim() : '';
        const deliveredTaskId = explicitTaskId ? undefined
          : [...(state.deliveries[caller] || [])].reverse().find((entry) => entry.taskId)?.taskId;
        const task = explicitTaskId
          ? state.tasks[explicitTaskId]
          : (deliveredTaskId ? state.tasks[deliveredTaskId] : undefined)
            ?? Object.values(state.tasks)
              .filter((entry) => entry.to === caller && ['running', 'sending', 'pending', 'queued'].includes(entry.status))
              .sort((a, b) => b.createdAt - a.createdAt)[0];
        if (!task) throw new Error('no active task for this session; pass taskId explicitly');
        if (task.to !== caller) throw new Error('that task is assigned to another session');
        const taskId = task.taskId;
        const status = STATUSES.includes(args.status) ? args.status : 'done';
        const summary = assertText(args.summary, 'summary');
        updateTask(taskId, { status, result: summary, reportedAt: Date.now() });
        const note = cfg.quietMode
          ? `【任务报告】${status === 'done' ? '✓' : status === 'blocked' ? '⏸' : '✗'} ${status} · ${task.title || taskId}：${trimText(summary, cfg.noticeTextLimit)}\n（报告已记入任务板，无需回复。）`
          : [
              '【任务报告 / task report】',
              `taskId: ${taskId}`,
              `worker: ${sessionLabel(caller)} (${caller})`,
              `status: ${status}`,
              '---',
              summary,
              '',
              '（任务报告，无需回复。）',
            ].join('\n');
        const messageId = `msg-${randomUUID()}`;
        const delivered = cfg.deliverReports === 'board'
          ? { mode: 'board-only' }
          : await deliver(task.from, note, { urgent: false, signal: exec?.signal });
        recordMessage({ messageId, taskId, from: caller, to: task.from, kind: 'result', hops: (task.hops || 1) + 1, status: cfg.deliverReports === 'board' ? 'board-only' : 'delivered', mode: delivered.mode, text: summary });
        void drainQueue(caller).catch((error) => {
          ctx.logger?.warn?.(`session-hub: draining queued tasks after report failed: ${String(error?.message || error)}`);
        });
        return { text: `task ${taskId} recorded as ${status}; dispatcher notified (${delivered.mode})`, taskId, status };
      },
    },

    session_task_status: {
      name: 'session_task_status',
      description: 'Read one task from the persistent task board (status, worker, dispatcher, result, timestamps). Read-only.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: { taskId: { type: 'string', description: 'Task id (omit to list all).' } },
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}) {
        const lines = [];
        const tasks = typeof args.taskId === 'string' && args.taskId.trim()
          ? [state.tasks[args.taskId]].filter(Boolean)
          : Object.values(state.tasks).sort((a, b) => b.createdAt - a.createdAt).slice(0, 30);
        if (tasks.length === 0) return { text: 'no tasks', tasks: [] };
        for (const task of tasks) {
          lines.push(`${task.taskId} | ${task.status} | ${sessionLabel(task.from)} → ${sessionLabel(task.to)} | ${task.title || ''}`);
          if (task.result) lines.push(`    result: ${trimText(task.result, 300)}`);
        }
        return { text: lines.join('\n'), tasks };
      },
    },

    session_team_status: {
      name: 'session_team_status',
      description: 'Overview of the session team: member roster (alias, session, online/running) plus the task board grouped by status. Read-only, does not wake anything.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: { limit: { type: 'integer', description: 'Max closed tasks to show (default 10).' } },
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const limit = Math.min(Math.max(Number(args.limit) || 10, 1), 50);
        const sessions = await listSessionSummaries(exec?.signal);
        const byId = new Map(sessions.map((s) => [s.sessionId, s]));
        const members = Object.entries(state.members).map(([sessionId, member]) => ({
          sessionId,
          alias: member.alias || Object.entries(state.aliases).find(([, id]) => id === sessionId)?.[0],
          title: byId.get(sessionId)?.title || sessionTitleCache.get(sessionId) || member.title,
          running: byId.get(sessionId)?.running === true,
          alive: byId.has(sessionId),
        }));
        const tasks = Object.values(state.tasks).sort((a, b) => b.createdAt - a.createdAt);
        const open = tasks.filter((task) => task.status === 'running' || task.status === 'pending');
        const blocked = tasks.filter((task) => task.status === 'blocked');
        const closed = tasks.filter((task) => ['done', 'failed', 'cancelled'].includes(task.status)).slice(0, limit);
        const lines = [];
        lines.push(`Members (${members.length}):`);
        for (const member of members) {
          lines.push(`- ${member.alias ? `@${member.alias}` : '(no alias)'} | ${member.title || '(untitled)'} | ${member.sessionId} | ${member.running ? 'running' : member.alive ? 'idle' : 'cold'} `);
        }
        lines.push('');
        lines.push(`Open tasks (${open.length}):`);
        for (const task of open.slice(0, 15)) lines.push(`- ${task.taskId} | ${task.status} | ${sessionLabel(task.from)} → ${sessionLabel(task.to)} | ${task.title || ''}`);
        lines.push('');
        lines.push(`Blocked (${blocked.length}):`);
        for (const task of blocked.slice(0, 10)) lines.push(`- ${task.taskId} | ${task.title || ''} | result: ${trimText(task.result || '', 120)}`);
        lines.push('');
        lines.push(`Recently closed (${closed.length}):`);
        for (const task of closed) lines.push(`- ${task.taskId} | ${task.status} | ${task.title || ''}`);
        return { text: lines.join('\n'), members, tasks: { open, blocked, closed } };
      },
    },

    session_alias: {
      name: 'session_alias',
      description: 'Bind a short alias to a session (or clear it with clear=true) so other tools can address it by name. Aliases persist across restarts.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          target: { type: 'string', description: 'Session id or unique title substring, or an existing alias to rename.' },
          alias: { type: 'string', description: 'New alias, e.g. ump-dev. Omit with clear=true to remove.' },
          clear: { type: 'boolean', description: 'Remove the alias instead of setting one.' },
        },
        required: ['target'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const resolved = await resolveTarget(args.target, exec?.signal);
        if (args.clear === true) {
          const alias = Object.entries(state.aliases).find(([, id]) => id === resolved.sessionId)?.[0];
          if (alias) delete state.aliases[alias];
          if (state.members[resolved.sessionId]) delete state.members[resolved.sessionId].alias;
          saveStateSoon();
          return { text: `alias cleared for ${resolved.sessionId}`, sessionId: resolved.sessionId };
        }
        const alias = assertText(String(args.alias ?? ''), 'alias');
        if (!/^[A-Za-z0-9._-]{1,40}$/.test(alias)) throw new Error('alias must match [A-Za-z0-9._-]{1,40}');
        const existing = state.aliases[alias];
        if (existing && existing !== resolved.sessionId) throw new Error(`alias "${alias}" already points to ${sessionLabel(existing)}`);
        state.aliases[alias] = resolved.sessionId;
        memberEntry(resolved.sessionId, { alias });
        saveStateNow();
        return { text: `@${alias} → ${resolved.sessionId}`, sessionId: resolved.sessionId, alias };
      },
    },

    session_bridge_log: {
      name: 'session_bridge_log',
      description: 'Recent cross-session timeline: messages, tasks, kinds, delivery modes and reply state. Read-only.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: { limit: { type: 'integer', description: 'Max entries (default 20).' } },
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}) {
        const limit = Math.min(Math.max(Number(args.limit) || 20, 1), 100);
        const entries = state.messages.slice(-limit).reverse();
        if (entries.length === 0) return { text: 'no cross-session messages yet', entries: [] };
        const lines = entries.map((entry) => `${new Date(entry.at).toLocaleTimeString()} | ${entry.kind} | ${sessionLabel(entry.from)} → ${sessionLabel(entry.to)}${entry.taskId ? ` | ${entry.taskId}` : ''} | ${entry.mode || ''}${entry.auto ? ' | auto' : ''}`);
        return { text: lines.join('\n'), entries };
      },
    },

    session_workspace_delete: {
      name: 'session_workspace_delete',
      description: 'Remove one workspace registration from the sidebar. The directory, its files and every session log are never touched. Refused while the workspace still has sessions unless force=true detaches the registration anyway (sessions become Ungrouped).',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          workspace: { type: 'string', description: 'Workspace id, exact title, or absolute directory path.' },
          force: { type: 'boolean', description: 'Detach the registration even if sessions are still attached.' },
        },
        required: ['workspace'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}) {
        const input = assertText(String(args.workspace ?? ''), 'workspace');
        const workspaces = ctx.workspaceRegistry.list();
        const workspace = workspaces.find((ws) => ws.id === input)
          || workspaces.find((ws) => ws.title === input)
          || workspaces.find((ws) => normalizeDir(ws.path) === normalizeDir(input));
        if (!workspace) throw new Error(`workspace "${input}" not found; use session_list to see workspaces`);
        if (workspace.sessionIds.length > 0 && args.force !== true) {
          throw new Error(`workspace "${workspace.title}" still has ${workspace.sessionIds.length} session(s); delete or move them first, or pass force=true to detach the registration`);
        }
        await ctx.workspaceRegistry.delete(workspace.id);
        return { text: `workspace "${workspace.title}" removed (directory untouched)`, workspaceId: workspace.id };
      },
    },

    session_hub_board: {
      name: 'session_hub_board',
      description: 'Return the URL of the live visual task board (auto-refreshing web page served by this Harness) plus current counts. Open the URL in a browser on this machine.',
      parameters: { type: 'object', additionalProperties: false, properties: {} },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute() {
        await boardPortReady();
        const url = `${boardOrigin()}/board`;
        const snapshot = await buildSnapshot();
        const counts = {};
        for (const task of snapshot.tasks) counts[task.status] = (counts[task.status] || 0) + 1;
        const countText = Object.entries(counts).map(([status, count]) => `${status}:${count}`).join(' ') || 'empty';
        return {
          text: `任务板（2 秒自动刷新，可嵌进 DSH 右侧栏的「浏览器」页签）: ${url}\n成员 ${snapshot.members.length}，任务 ${snapshot.tasks.length}（${countText}）\n打开方式：右侧栏 → 指南/Guide → 浏览器 → 粘贴上面的地址。该地址是独立 loopback 端口（不是 DSH 自身 origin），内置浏览器可以打开。`,
          url,
        };
      },
    },

    session_broadcast: {
      name: 'session_broadcast',
      description: 'Send one message to every team member, or to a listed subset of targets. Each target is its own delivery (rate/hop limits apply). kind=task creates one tracked task per target and obeys each worker\'s serial queue. Use it for announcements or fan-out work.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          message: { type: 'string', description: 'Message body.' },
          targets: { type: 'array', items: { type: 'string' }, description: 'Optional aliases/session ids; omit to broadcast to every known member.' },
          kind: { type: 'string', enum: ['note', 'task', 'question'], description: 'Message kind (default note).' },
          urgent: { type: 'boolean', description: 'Steer at the next step for running targets.' },
        },
        required: ['message'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const caller = callerSessionId(exec);
        const body = assertText(args.message, 'message');
        const kind = ['note', 'task', 'question'].includes(args.kind) ? args.kind : 'note';
        const requested = Array.isArray(args.targets) && args.targets.length > 0
          ? args.targets
          : Object.entries(state.members).map(([sessionId, member]) => member.alias || sessionId);
        if (!requested.length) throw new Error('no members to broadcast to: create or join sessions first');
        if (requested.length > MAX_BROADCAST_TARGETS) {
          throw new Error(`too many broadcast targets (${requested.length} > ${MAX_BROADCAST_TARGETS})`);
        }
        const results = [];
        const resolvedTargets = [];
        const seen = new Set();
        for (const raw of requested) {
          if (String(raw).length > MAX_SMALL_TEXT) {
            results.push({ target: String(raw).slice(0, 40), error: `target is too long (max ${MAX_SMALL_TEXT} characters)` });
            continue;
          }
          let sessionId;
          try {
            const resolved = await resolveTarget(String(raw), exec?.signal);
            sessionId = resolved.sessionId;
          } catch (error) {
            results.push({ target: String(raw).slice(0, 80), error: safeErrorText(error) });
            continue;
          }
          if (sessionId === caller || seen.has(sessionId)) continue;
          seen.add(sessionId);
          resolvedTargets.push(sessionId);
        }

        // Approval policy applies to broadcasts exactly as it does to session_send:
        // one question covers every protected target of this fan-out.
        const cwdIndex = await sessionCwdIndex(exec?.signal);
        const protectedIds = resolvedTargets.filter((sessionId) => approvalRequiredFor(cwdIndex.get(sessionId) || ''));
        let broadcastDecision;
        if (protectedIds.length > 0) {
          const cwds = [...new Set(protectedIds.map((sessionId) => cwdIndex.get(sessionId) || ''))];
          broadcastDecision = await askDispatchApproval(exec, {
            kind,
            cwd: cwds.join(' | '),
            targetLabel: protectedIds.map((sessionId) => sessionLabel(sessionId)).join(', '),
            title: body.slice(0, 60),
            body,
          });
        }

        for (const sessionId of resolvedTargets) {
          const targetCwd = cwdIndex.get(sessionId) || '';
          const needsApproval = protectedIds.includes(sessionId);
          try {
            if (needsApproval && !broadcastDecision?.approved) {
              const status = broadcastDecision?.rejected ? 'rejected' : 'awaiting-approval';
              if (kind === 'task') {
                const taskId = `task-${randomUUID()}`;
                state.tasks[taskId] = {
                  taskId,
                  title: body.slice(0, 80),
                  status,
                  from: caller,
                  to: sessionId,
                  hops: 1,
                  body,
                  urgent: args.urgent === true,
                  approval: { required: true, cwd: targetCwd, askedAt: Date.now() },
                  ...(broadcastDecision?.reason ? { result: broadcastDecision.reason } : {}),
                  createdAt: Date.now(),
                  updatedAt: Date.now(),
                };
                saveStateSoon();
                results.push({ target: sessionLabel(sessionId), sessionId, taskId, awaitingApproval: status === 'awaiting-approval', rejected: status === 'rejected' });
              } else {
                results.push({ target: sessionLabel(sessionId), sessionId, awaitingApproval: status === 'awaiting-approval', rejected: status === 'rejected' });
              }
              continue;
            }
            if (kind === 'task') {
              const taskId = `task-${randomUUID()}`;
              state.tasks[taskId] = {
                taskId,
                title: body.slice(0, 80),
                status: 'queued',
                from: caller,
                to: sessionId,
                hops: 1,
                body,
                urgent: args.urgent === true,
                ...(needsApproval ? { approval: { required: true, approvedAt: Date.now(), cwd: targetCwd } } : {}),
                createdAt: Date.now(),
                updatedAt: Date.now(),
              };
              const active = activeTaskFor(sessionId);
              if (active) {
                saveStateSoon();
                results.push({ target: sessionLabel(sessionId), sessionId, taskId, queued: true });
                continue;
              }
              const dispatched = await dispatchTask(state.tasks[taskId]);
              if (dispatched.busy) {
                saveStateSoon();
                results.push({ target: sessionLabel(sessionId), sessionId, taskId, queued: true });
                continue;
              }
              results.push({ target: sessionLabel(sessionId), sessionId, taskId, ...dispatched });
            } else {
              const messageId = `msg-${randomUUID()}`;
              const text = [
                envelope({ kind, messageId, from: caller, fromLabel: sessionLabel(caller), toLabel: sessionLabel(sessionId), text: body, hops: 1 }),
                guidance({ kind, messageId }),
              ].join('\n');
              const delivered = await deliver(sessionId, text, { urgent: args.urgent === true, signal: exec?.signal });
              recordMessage({ messageId, from: caller, to: sessionId, kind, hops: 1, status: 'delivered', mode: delivered.mode, text: body });
              results.push({ target: sessionLabel(sessionId), sessionId, messageId, mode: delivered.mode });
            }
          } catch (error) {
            results.push({ target: sessionLabel(sessionId), error: safeErrorText(error) });
          }
        }
        const ok = results.filter((result) => !result.error).length;
        const lines = [`broadcast ${kind} delivered to ${ok}/${results.length} target(s)`];
        for (const result of results) {
          const detail = result.error ? ` · error: ${result.error}`
            : result.awaitingApproval ? ' · 待用户批准'
              : result.rejected ? ' · 用户拒绝'
                : result.queued ? ' · queued'
                  : result.taskId ? ` · task ${result.taskId}`
                    : ` · ${result.mode || ''}`;
          lines.push(`- ${result.target}${detail}`);
        }
        return { text: lines.join('\n'), results };
      },
    },

    session_inbox: {
      name: 'session_inbox',
      description: 'Read cross-session messages addressed to this session without adding anything to the transcript. Use it to catch up on task reports / completion notices kept board-only. Marks them read unless markRead=false.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          limit: { type: 'integer', description: 'Max messages to return (default 20).' },
          markRead: { type: 'boolean', description: 'Mark returned messages as read (default true).' },
        },
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const caller = callerSessionId(exec);
        const limit = Math.min(Math.max(Number(args.limit) || 20, 1), 100);
        const markRead = args.markRead !== false;
        const since = Number(state.readMarks?.[caller] || 0);
        const pending = state.messages.filter((entry) => entry.to === caller && Number(entry.at) > since);
        const seen = pending.slice(-limit).sort((a, b) => b.at - a.at);
        if (seen.length === 0) return { text: 'no unread cross-session messages', messages: [] };
        const lines = seen.map((entry) => {
          const time = new Date(entry.at).toLocaleTimeString();
          return `- ${time} | ${entry.kind}${entry.auto ? ' · auto' : ''} | ${sessionLabel(entry.from)} | ${trimText(String(entry.text || ''), 200)}`;
        });
        if (markRead) {
          state.readMarks = state.readMarks || {};
          state.readMarks[caller] = Math.max(...seen.map((entry) => Number(entry.at) || 0));
          saveStateNow();
        }
        return { text: `inbox: ${seen.length} message(s)${markRead ? ' (marked read)' : ''}\n${lines.join('\n')}`, messages: seen };
      },
    },

    session_purge_log: {
      name: 'session_purge_log',
      description: 'Clear board history: message ledger and/or finished tasks (active tasks and approvals are never removed). Use it to tidy up test noise on the board.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          scope: { type: 'string', enum: ['messages', 'tasks', 'all'], description: 'What to clear (default messages).' },
        },
        required: ['scope'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}) {
        const scope = ['messages', 'tasks', 'all'].includes(args.scope) ? args.scope : 'messages';
        let messages = 0;
        let tasks = 0;
        if (scope === 'messages' || scope === 'all') {
          messages = state.messages.length;
          state.messages = [];
        }
        if (scope === 'tasks' || scope === 'all') {
          for (const [taskId, task] of Object.entries(state.tasks)) {
            if (['running', 'sending', 'queued', 'awaiting-approval'].includes(task.status)) continue;
            delete state.tasks[taskId];
            tasks++;
          }
        }
        saveStateNow();
        return { text: `cleared ${messages} message(s) and ${tasks} finished task(s) from the board history`, messages, tasks };
      },
    },

    session_export_log: {
      name: 'session_export_log',
      description: 'Export the cross-session timeline and task board to a Markdown file (default under $DSH_HOME/session-hub/). Read-only snapshot; returns the file path.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: { path: { type: 'string', description: 'Optional absolute output path.' } },
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}) {
        const target = resolveExportTarget(args.path);
        const tasks = Object.values(state.tasks).sort((a, b) => b.createdAt - a.createdAt);
        const lines = [
          '# Session Hub timeline',
          '',
          `Exported: ${new Date().toISOString()}`,
          `Members: ${Object.keys(state.members).length} · Tasks: ${tasks.length} · Messages: ${state.messages.length}`,
          '',
          '## Tasks',
        ];
        for (const task of tasks) {
          lines.push(`- ${task.taskId} | ${task.status} | ${sessionLabel(task.from)} → ${sessionLabel(task.to)} | ${task.title || ''}`);
          if (task.result) lines.push(`  - result: ${trimText(String(task.result), 400)}`);
        }
        lines.push('', '## Messages');
        for (const entry of state.messages.slice().reverse()) {
          lines.push(`- ${new Date(entry.at).toISOString()} | ${entry.kind} | ${sessionLabel(entry.from)} → ${sessionLabel(entry.to)}${entry.taskId ? ` | ${entry.taskId}` : ''} | ${entry.mode || ''}${entry.auto ? ' | auto' : ''}`);
        }
        fs.mkdirSync(path.dirname(target), { recursive: true });
        fs.writeFileSync(target, lines.join('\n'), 'utf8');
        return { text: `exported ${tasks.length} task(s) / ${state.messages.length} message(s) to ${target}`, path: target };
      },
    },

    session_task_approve: {
      name: 'session_task_approve',
      description: 'Approve (or reject) one task that is waiting for user approval because its target workspace matches the approval policy. Only the dispatcher of that task may approve it. Use it when the user has explicitly confirmed in the conversation.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          taskId: { type: 'string', description: 'Task id awaiting approval.' },
          approve: { type: 'boolean', description: 'true (default) dispatches the task; false marks it rejected.' },
        },
        required: ['taskId'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const caller = callerSessionId(exec);
        const taskId = assertText(String(args.taskId ?? ''), 'taskId');
        const task = state.tasks[taskId];
        if (!task) throw new Error(`unknown taskId "${taskId}"`);
        if (task.from !== caller) throw new Error('only the dispatcher of this task may approve it');
        if (task.status !== 'awaiting-approval' && task.status !== 'rejected') {
          return { text: `task ${taskId} is ${task.status}; nothing to approve`, taskId };
        }
        if (args.approve === false) {
          updateTask(taskId, { status: 'rejected', result: 'rejected by user' });
          saveStateNow();
          return { text: `task ${taskId} rejected`, taskId };
        }
        updateTask(taskId, { status: 'queued', approval: { ...(task.approval || {}), approvedAt: Date.now() } });
        const active = activeTaskFor(task.to);
        if (active) {
          saveStateNow();
          return { text: `task ${taskId} approved and queued behind ${active.taskId}`, taskId, queued: true };
        }
        const dispatched = await dispatchTask(state.tasks[taskId]);
        if (dispatched.busy) {
          saveStateSoon();
          return { text: `task ${taskId} approved and queued — the worker just started another dispatch`, taskId, queued: true };
        }
        if (!dispatched.ok) throw new Error(dispatched.error);
        return { text: `task ${taskId} approved and dispatched (${dispatched.mode})`, taskId, messageId: dispatched.messageId };
      },
    },

    session_task_cancel: {
      name: 'session_task_cancel',
      description: 'Cancel one task on the board. Only the dispatcher or the worker of that task may cancel it. This marks the task cancelled; it does not interrupt the worker session itself.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: { taskId: { type: 'string', description: 'Task id to cancel.' } },
        required: ['taskId'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const caller = callerSessionId(exec);
        const taskId = assertText(String(args.taskId ?? ''), 'taskId');
        const task = state.tasks[taskId];
        if (!task) throw new Error(`unknown taskId "${taskId}"`);
        if (task.from !== caller && task.to !== caller) throw new Error('only the dispatcher or the worker of this task may cancel it');
        if (['done', 'failed', 'cancelled'].includes(task.status)) return { text: `task ${taskId} is already ${task.status}`, taskId };
        updateTask(taskId, { status: 'cancelled', result: task.result || `cancelled by ${sessionLabel(caller)}`, cancelledAt: Date.now() });
        return { text: `task ${taskId} cancelled`, taskId };
      },
    },

    session_delete: {
      name: 'session_delete',
      description: 'Permanently delete one session (its history is removed) and drop it from the board. With deleteWorkspace=true and no other session left in its workspace, also remove that workspace registration (the directory and its files are never touched). Only use when the user explicitly asked to clean up.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          target: { type: 'string', description: 'Session id or alias to delete.' },
          deleteWorkspace: { type: 'boolean', description: 'Also remove the workspace registration when it becomes empty.' },
        },
        required: ['target'],
      },
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: value?.text || 'ok' }],
      async execute(args = {}, exec) {
        const caller = callerSessionId(exec);
        const resolved = await resolveTarget(args.target, exec?.signal);
        if (resolved.sessionId === caller) throw new Error('refusing to delete the calling session; call this from another session');
        const sessionId = resolved.sessionId;
        const owningWorkspace = args.deleteWorkspace === true
          ? ctx.workspaceRegistry.list().find((candidate) => candidate.sessionIds.includes(sessionId))
          : undefined;
        await ctx.sessionController.delete({ sessionId });
        try {
          for (const [alias, id] of Object.entries(state.aliases)) if (id === sessionId) delete state.aliases[alias];
          delete state.members[sessionId];
          for (const [taskId, task] of Object.entries(state.tasks)) if (task.from === sessionId || task.to === sessionId) delete state.tasks[taskId];
          state.messages = state.messages.filter((entry) => entry.from !== sessionId && entry.to !== sessionId);
        } finally {
          // Persist the cleanup even if a later workspace step throws, so memory and
          // disk never disagree after a partial failure.
          saveStateNow();
        }
        let removedWorkspace;
        if (owningWorkspace) {
          try {
            const remaining = ctx.workspaceRegistry.get(owningWorkspace.id)?.sessionIds ?? [];
            if (remaining.length === 0) {
              await ctx.workspaceRegistry.delete(owningWorkspace.id);
              removedWorkspace = owningWorkspace.title;
            }
          } catch (error) {
            ctx.logger?.warn?.(`session-hub: session ${sessionId} deleted but workspace cleanup failed: ${String(error?.message || error)}`);
          }
        }
        return {
          text: `deleted session ${sessionId}${removedWorkspace ? ` and workspace "${removedWorkspace}"` : ''}`,
          sessionId,
          removedWorkspace,
        };
      },
    },
  };

  const lossless = (value) => JSON.parse(JSON.stringify(value ?? {}));

  for (const definition of Object.values(T)) {
    ctx.tools.register({
      name: definition.name,
      description: definition.description,
      parameters: definition.parameters,
      output: {
        schema: definition.schema,
        render: (args, value) => (typeof definition.render === 'function'
          ? definition.render(args, value)
          : [{ type: 'text', text: String(value?.text ?? 'ok') }]),
      },
      execute: async (args, exec) => lossless(await definition.execute(args, exec)),
    });
  }

  // ---------------------------------------------------------------- live web board

  const BOARD_PATH = '/session-hub/board';
  const SNAPSHOT_PATH = '/session-hub/snapshot';

  // The in-app right-sidebar browser refuses the DSH app's own origin, so the board
  // is also served from an independent loopback port (a different origin).
  let standaloneBoard;

  /** Origins allowed to frame the board (the DSH web app itself). */
  function frameAncestors() {
    const origins = new Set(['http://127.0.0.1:43129', 'http://localhost:43129']);
    try {
      const server = ctx.get('webServer');
      const port = server?.port;
      if (port) {
        origins.add(`http://127.0.0.1:${port}`);
        origins.add(`http://localhost:${port}`);
      }
    } catch {
      /* keep the default origins */
    }
    return [...origins];
  }

  function boardSecurityHeaders(extra = {}) {
    return {
      'x-content-type-options': 'nosniff',
      'referrer-policy': 'no-referrer',
      'cache-control': 'no-store',
      ...extra,
    };
  }

  function startStandaloneBoard() {
    try {
      const server = http.createServer((req, res) => {
        const url = new URL(req.url ?? '/', 'http://127.0.0.1');
        const address = String(req.socket?.remoteAddress ?? '');
        const plain = address.startsWith('::ffff:') ? address.slice(7) : address;
        if (!(plain === '::1' || plain.startsWith('127.'))) {
          res.writeHead(403);
          res.end('forbidden');
          return;
        }
        if (req.method !== 'GET' && req.method !== 'HEAD') {
          res.writeHead(405, { allow: 'GET, HEAD' });
          res.end();
          return;
        }
        if (url.pathname === '/' || url.pathname === '/board') {
          const body = Buffer.from(boardHtml(), 'utf8');
          res.writeHead(200, boardSecurityHeaders({
            'content-type': 'text/html; charset=utf-8',
            'content-length': String(body.byteLength),
            'content-security-policy': `default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors ${frameAncestors().join(' ')}`,
          }));
          res.end(req.method === 'HEAD' ? undefined : body);
          return;
        }
        if (url.pathname === '/snapshot') {
          void buildSnapshot(url.searchParams.get('session') || undefined).then(
            (value) => {
              const body = Buffer.from(JSON.stringify(value), 'utf8');
              res.writeHead(200, boardSecurityHeaders({
                'content-type': 'application/json; charset=utf-8',
                'content-length': String(body.byteLength),
              }));
              res.end(body);
            },
            (error) => {
              res.writeHead(500);
              res.end(String(error));
            },
          );
          return;
        }
        res.writeHead(404);
        res.end();
      });
      server.on('error', (error) => {
        ctx.logger?.warn?.(`session-hub: standalone board server failed: ${String(error?.message || error)}`);
      });
      server.on('listening', () => {
        const address = server.address();
        const port = typeof address === 'object' && address ? address.port : undefined;
        if (port) standaloneBoard = { server, port };
        ctx.logger?.info?.(`session-hub: standalone board listening on port ${String(port)}`);
      });
      ctx.effect(() => () => {
        try { server.close(); } catch { /* ignore */ }
      }, 'session-hub.standalone-board');
      server.listen(0, '127.0.0.1');
    } catch (error) {
      ctx.logger?.warn?.(`session-hub: standalone board unavailable: ${String(error?.message || error)}`);
    }
  }

  async function boardPortReady(timeoutMs = 1500) {
    const deadline = Date.now() + timeoutMs;
    while (!standaloneBoard?.port && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    return standaloneBoard?.port;
  }

  function boardOrigin() {
    if (standaloneBoard?.port) return `http://127.0.0.1:${standaloneBoard.port}`;
    try {
      const server = ctx.get('webServer');
      const port = server?.port;
      if (port) return `http://127.0.0.1:${port}`;
    } catch {
      /* fall through to the default desktop port */
    }
    return 'http://127.0.0.1:43129';
  }

  /** Human label for the board: alias first, then the live session title, never a bare snowflake id. */
  function labelForSnapshot(sessionId, byId) {
    const alias = Object.entries(state.aliases).find(([, id]) => id === sessionId)?.[0];
    if (alias) return alias;
    const member = state.members[sessionId];
    if (member?.alias) return member.alias;
    const summary = byId?.get(sessionId);
    if (summary?.title) return summary.title;
    const raw = String(sessionId || '');
    return raw.startsWith('session-') ? `会话 ${raw.slice(8, 16)}` : (raw || '未知会话');
  }

  async function buildSnapshot(filterSessionId) {
    const scoped = typeof filterSessionId === 'string' && filterSessionId.trim() ? filterSessionId.trim() : undefined;
    let sessions = [];
    try {
      sessions = await listSessionSummaries(undefined);
    } catch (error) {
      ctx.logger?.info?.(`session-hub: session list unavailable for board snapshot: ${safeErrorText(error)}`);
      sessions = [];
    }
    const byId = new Map(sessions.map((session) => [session.sessionId, session]));
    const allTasks = Object.values(state.tasks);
    const scopedTasks = scoped
      ? allTasks.filter((task) => task.from === scoped || task.to === scoped)
      : allTasks;
    const tasks = scopedTasks
      .sort((a, b) => b.createdAt - a.createdAt)
      .slice(0, 200)
      .map((task) => ({ ...task, fromLabel: labelForSnapshot(task.from, byId), toLabel: labelForSnapshot(task.to, byId) }));
    for (const [sessionId] of Object.entries(state.members)) {
      if (byId.get(sessionId)?.title) continue;
      try {
        const snapshot = await ctx.sessionQuery.readTitle(sessionId);
        const title = typeof snapshot === 'string' ? snapshot
          : (snapshot && typeof snapshot.title === 'string' ? snapshot.title : undefined);
        if (title) {
          byId.set(sessionId, { ...(byId.get(sessionId) || { sessionId, cwd: '', running: false, agentAvailable: false }), title });
          sessionTitleCache.set(sessionId, title);
          if (state.members[sessionId] && !state.members[sessionId].title) state.members[sessionId].title = title;
        }
      } catch (error) {
        ctx.logger?.info?.(`session-hub: member title unavailable: ${safeErrorText(error)}`);
      }
    }
    const involved = scoped ? new Set([scoped]) : undefined;
    if (involved) {
      for (const task of scopedTasks) { involved.add(task.from); involved.add(task.to); }
      for (const entry of state.messages) {
        if (entry.from === scoped || entry.to === scoped) { involved.add(entry.from); involved.add(entry.to); }
      }
    }
    const members = Object.entries(state.members)
      .filter(([sessionId]) => !involved || involved.has(sessionId))
      .map(([sessionId, member]) => ({
        sessionId,
        alias: member.alias || Object.entries(state.aliases).find(([, id]) => id === sessionId)?.[0],
        title: byId.get(sessionId)?.title || sessionTitleCache.get(sessionId) || member.title,
        running: byId.get(sessionId)?.running === true,
        alive: byId.has(sessionId),
      }));
    return {
      generatedAt: Date.now(),
      aliases: { ...state.aliases },
      members,
      tasks,
      messages: (scoped ? state.messages.filter((entry) => entry.from === scoped || entry.to === scoped) : state.messages).slice(-60).reverse().map((entry) => ({
        at: entry.at,
        messageId: entry.messageId,
        kind: entry.kind,
        from: labelForSnapshot(entry.from, byId),
        to: labelForSnapshot(entry.to, byId),
        taskId: entry.taskId,
        mode: entry.mode,
        status: entry.status,
        auto: entry.auto === true,
        text: typeof entry.text === 'string' ? entry.text : '',
      })),
    };
  }

  const BOARD_HTML = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Session Hub · 任务协作中心</title>
<style>
:root{--bg-primary:#0a0e14;--bg-secondary:#101419;--bg-tertiary:#1a1f29;--bg-card:#151921;--bg-hover:#1f2430;--border-subtle:#262b35;--border-medium:#2d3340;--border-strong:#3d4350;--text-primary:#e6eef3;--text-secondary:#8b95a5;--text-muted:#5f6875;--text-link:#58a6ff;--status-queued:#8957e5;--status-running:#1f6feb;--status-done:#2ea043;--status-blocked:#d29922;--status-failed:#da3633;--status-cancelled:#6e7681;--status-approve:#b083f0;--shadow-sm:0 1px 3px rgba(0,0,0,0.3);--shadow-md:0 4px 12px rgba(0,0,0,0.4);--shadow-lg:0 8px 24px rgba(0,0,0,0.5);--transition-fast:150ms cubic-bezier(0.4,0,0.2,1);--transition-base:250ms cubic-bezier(0.4,0,0.2,1)}*{box-sizing:border-box;margin:0;padding:0}body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei","PingFang SC",sans-serif;background:var(--bg-primary);color:var(--text-primary);line-height:1.5;overflow-x:hidden}.navbar{background:var(--bg-secondary);border-bottom:1px solid var(--border-medium);padding:0 24px;height:60px;display:flex;align-items:center;gap:20px;position:sticky;top:0;z-index:100;backdrop-filter:blur(8px)}.navbar-brand{display:flex;align-items:center;gap:12px;font-size:18px;font-weight:600}.status-dot{width:10px;height:10px;border-radius:50%;background:var(--status-done);box-shadow:0 0 12px var(--status-done);animation:pulse 2s infinite}@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}.navbar-stats{display:flex;gap:24px;margin-left:auto;font-size:13px;color:var(--text-secondary)}.stat-item{display:flex;align-items:center;gap:6px}.stat-label{color:var(--text-muted)}.stat-value{color:var(--text-primary);font-weight:600}.container{max-width:1920px;margin:0 auto;padding:24px}.tabs{display:flex;gap:4px;margin-bottom:24px;border-bottom:1px solid var(--border-medium)}.tab{padding:12px 20px;background:none;border:none;color:var(--text-secondary);font-size:14px;font-weight:500;cursor:pointer;border-bottom:2px solid transparent;transition:all var(--transition-fast);position:relative}.tab:hover{color:var(--text-primary);background:var(--bg-hover)}.tab.active{color:var(--text-primary);border-bottom-color:var(--status-running)}.tab-badge{display:inline-block;margin-left:6px;padding:2px 8px;background:var(--bg-tertiary);border-radius:12px;font-size:11px;font-weight:600}.tab-content{display:none}.tab-content.active{display:block;animation:fadeIn var(--transition-base)}@keyframes fadeIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}.members-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:16px;margin-bottom:24px}.member-card{background:var(--bg-card);border:1px solid var(--border-subtle);border-radius:12px;padding:16px;transition:all var(--transition-fast);position:relative;overflow:hidden}.member-card:hover{border-color:var(--border-strong);box-shadow:var(--shadow-md);transform:translateY(-2px)}.member-card::before{content:'';position:absolute;top:0;left:0;width:4px;height:100%;background:var(--status-done)}.member-card.running::before{background:var(--status-running);animation:shimmer 2s infinite}@keyframes shimmer{0%,100%{opacity:1}50%{opacity:0.3}}.member-card.cold::before{background:var(--text-muted)}.member-header{display:flex;align-items:center;gap:12px;margin-bottom:12px}.member-avatar{width:40px;height:40px;border-radius:10px;background:linear-gradient(135deg,var(--status-running),var(--status-queued));display:flex;align-items:center;justify-content:center;font-weight:600;font-size:16px;flex-shrink:0}.member-info{flex:1;min-width:0}.member-name{font-size:15px;font-weight:600;margin-bottom:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.member-title{font-size:13px;color:var(--text-secondary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.member-status{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px}.member-status.running{background:rgba(31,111,235,0.15);color:#58a6ff}.member-status.idle{background:rgba(139,149,165,0.15);color:var(--text-secondary)}.member-status.cold{background:rgba(95,104,117,0.15);color:var(--text-muted)}.member-meta{margin-top:12px;padding-top:12px;border-top:1px solid var(--border-subtle);font-size:12px;color:var(--text-muted);font-family:"JetBrains Mono","Fira Code",monospace}.kanban-board{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;align-items:start}.kanban-column{background:var(--bg-secondary);border:1px solid var(--border-subtle);border-radius:12px;padding:16px;min-height:200px}.column-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;padding-bottom:12px;border-bottom:2px solid var(--border-medium)}.column-title{display:flex;align-items:center;gap:8px;font-size:14px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px}.column-icon{width:20px;height:20px;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:12px}.column-count{padding:4px 10px;background:var(--bg-tertiary);border-radius:12px;font-size:12px;font-weight:600;color:var(--text-secondary)}.column-cards{display:flex;flex-direction:column;gap:12px}.task-card{background:var(--bg-card);border:1px solid var(--border-subtle);border-left:3px solid var(--status-running);border-radius:10px;padding:14px;cursor:pointer;transition:all var(--transition-fast);position:relative}.task-card:hover{border-color:var(--border-strong);box-shadow:var(--shadow-md);transform:translateY(-2px)}.task-card.queued{border-left-color:var(--status-queued)}.task-card.running{border-left-color:var(--status-running)}.task-card.blocked{border-left-color:var(--status-blocked)}.task-card.done{border-left-color:var(--status-done)}.task-card.failed{border-left-color:var(--status-failed)}.task-card.cancelled{border-left-color:var(--status-cancelled)}.task-header{display:flex;align-items:flex-start;gap:8px;margin-bottom:10px}.task-title{flex:1;font-size:14px;font-weight:600;line-height:1.4;word-break:break-word}.task-badge{display:inline-flex;align-items:center;padding:3px 8px;border-radius:6px;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.3px;background:rgba(176,131,240,0.15);color:var(--status-approve);flex-shrink:0}.task-route{display:flex;align-items:center;gap:6px;margin-bottom:8px;font-size:12px;color:var(--text-secondary)}.task-route-arrow{color:var(--text-muted)}.task-result{margin-top:10px;padding:10px;background:var(--bg-tertiary);border-radius:8px;font-size:12px;color:var(--text-secondary);line-height:1.5;word-break:break-word}.task-result-label{color:var(--text-muted);font-weight:600;margin-bottom:4px}.task-footer{display:flex;align-items:center;justify-content:space-between;margin-top:10px;padding-top:10px;border-top:1px solid var(--border-subtle);font-size:11px;color:var(--text-muted)}.task-id{font-family:"JetBrains Mono","Fira Code",monospace}.task-time{display:flex;align-items:center;gap:4px}.timeline{max-height:600px;overflow-y:auto;padding-right:8px}.timeline::-webkit-scrollbar{width:8px}.timeline::-webkit-scrollbar-track{background:var(--bg-secondary);border-radius:4px}.timeline::-webkit-scrollbar-thumb{background:var(--border-strong);border-radius:4px}.timeline::-webkit-scrollbar-thumb:hover{background:var(--text-muted)}.timeline-item{display:grid;grid-template-columns:80px 60px 1fr;gap:12px;padding:12px 16px;margin-bottom:8px;background:var(--bg-card);border:1px solid var(--border-subtle);border-radius:10px;font-size:13px;transition:all var(--transition-fast)}.timeline-item:hover{border-color:var(--border-medium);background:var(--bg-hover)}.timeline-time{color:var(--text-muted);font-family:"JetBrains Mono","Fira Code",monospace;font-size:12px}.timeline-kind{display:inline-flex;align-items:center;padding:3px 8px;border-radius:6px;font-size:11px;font-weight:600;background:var(--bg-tertiary);color:var(--text-link)}.timeline-content{color:var(--text-secondary)}.empty-state{text-align:center;padding:60px 20px;color:var(--text-muted)}.empty-icon{font-size:48px;margin-bottom:16px;opacity:0.3}.empty-text{font-size:14px}.footer{margin-top:40px;padding:20px 24px;text-align:center;color:var(--text-muted);font-size:12px;border-top:1px solid var(--border-subtle)}@media (max-width:1200px){.kanban-board{grid-template-columns:repeat(auto-fit,minmax(240px,1fr))}}@media (max-width:768px){.navbar-stats{display:none}.members-grid{grid-template-columns:1fr}.kanban-board{grid-template-columns:1fr}.timeline-item{grid-template-columns:60px 50px 1fr;font-size:12px}}
</style>
</head>
<body>
<nav class="navbar">
<div class="navbar-brand"><span class="status-dot"></span><span>Session Hub</span></div>
<div class="navbar-stats">
<div class="stat-item"><span class="stat-label">成员</span><span class="stat-value" id="stat-members">0</span></div>
<div class="stat-item"><span class="stat-label">任务</span><span class="stat-value" id="stat-tasks">0</span></div>
<div class="stat-item"><span class="stat-label">消息</span><span class="stat-value" id="stat-messages">0</span></div>
<div class="stat-item"><span class="stat-label">更新于</span><span class="stat-value" id="stat-time">--:--:--</span></div>
</div>
</nav>
<div class="container">
<div class="tabs">
<button class="tab active" data-tab="members">成员<span class="tab-badge" id="badge-members">0</span></button>
<button class="tab" data-tab="kanban">任务看板<span class="tab-badge" id="badge-tasks">0</span></button>
<button class="tab" data-tab="timeline">消息时间线<span class="tab-badge" id="badge-messages">0</span></button>
</div>
<div class="tab-content active" id="content-members">
<div class="members-grid" id="members-grid">
<div class="empty-state"><div class="empty-icon">👥</div><div class="empty-text">暂无成员</div></div>
</div>
</div>
<div class="tab-content" id="content-kanban">
<div class="kanban-board" id="kanban-board"></div>
</div>
<div class="tab-content" id="content-timeline">
<div class="timeline" id="timeline">
<div class="empty-state"><div class="empty-icon">📝</div><div class="empty-text">暂无消息</div></div>
</div>
</div>
</div>
<footer class="footer"><p>Session Hub v0.1.1 · 每2秒自动刷新 · 数据来源 DSH session-hub 插件</p></footer>
<script>
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const ago=ts=>{const s=Math.max(0,Math.round((Date.now()-ts)/1000));if(s<60)return s+'s';const m=Math.round(s/60);if(m<60)return m+'m';const h=Math.round(m/60);if(h<24)return h+'h';return Math.round(h/24)+'d';};
const getInitials=n=>{if(!n)return'?';const c=n.replace(/^@/,'');return c.slice(0,2).toUpperCase();};
const getAvatarColor=sid=>{const colors=['linear-gradient(135deg,#667eea 0%,#764ba2 100%)','linear-gradient(135deg,#f093fb 0%,#f5576c 100%)','linear-gradient(135deg,#4facfe 0%,#00f2fe 100%)','linear-gradient(135deg,#43e97b 0%,#38f9d7 100%)','linear-gradient(135deg,#fa709a 0%,#fee140 100%)','linear-gradient(135deg,#30cfd0 0%,#330867 100%)'];const hash=sid.split('').reduce((a,c)=>a+c.charCodeAt(0),0);return colors[hash%colors.length];};
document.querySelectorAll('.tab').forEach(tab=>{tab.addEventListener('click',()=>{const target=tab.dataset.tab;document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));tab.classList.add('active');document.getElementById(\`content-\${target}\`).classList.add('active');});});
function renderMemberCard(m){const sc=m.running?'running':(m.alive?'idle':'cold');const st=m.running?'Running':(m.alive?'Idle':'Cold');return\`<div class="member-card \${sc}"><div class="member-header"><div class="member-avatar" style="background:\${getAvatarColor(m.sessionId)}">\${getInitials(m.alias||m.title)}</div><div class="member-info"><div class="member-name">\${esc(m.alias?'@'+m.alias:'(未命名)')}</div><div class="member-title">\${esc(m.title||'(无标题)')}</div></div><span class="member-status \${sc}">\${st}</span></div><div class="member-meta">\${esc(m.sessionId.slice(0,16))}...</div></div>\`;}
function renderTaskCard(t){const sc=t.status==='awaiting-approval'?'queued':t.status;const ha=t.status==='awaiting-approval';return\`<div class="task-card \${sc}"><div class="task-header"><div class="task-title">\${esc(t.title||t.taskId)}</div>\${ha?'<span class="task-badge">待批准</span>':''}</div><div class="task-route"><span>\${esc(t.fromLabel)}</span><span class="task-route-arrow">→</span><span>\${esc(t.toLabel)}</span></div>\${t.result?\`<div class="task-result"><div class="task-result-label">结果:</div>\${esc(String(t.result).slice(0,160))}</div>\`:''}<div class="task-footer"><span class="task-id">\${esc(t.taskId.slice(0,12))}</span><span class="task-time">\${ago(t.createdAt)}</span></div></div>\`;}
function renderKanbanColumn(title,icon,tasks,sk){const icons={queued:'⏱️',running:'⚡',blocked:'⚠️',done:'✅',failed:'❌'};return\`<div class="kanban-column"><div class="column-header"><div class="column-title"><span class="column-icon">\${icons[sk]||'📋'}</span>\${title}</div><span class="column-count">\${tasks.length}</span></div><div class="column-cards">\${tasks.length>0?tasks.map(renderTaskCard).join(''):'<div class="empty-state"><div class="empty-icon">📭</div><div class="empty-text">暂无任务</div></div>'}</div></div>\`;}
function renderTimelineItem(e){return\`<div class="timeline-item"><span class="timeline-time">\${new Date(e.at).toLocaleTimeString()}</span><span class="timeline-kind">\${esc(e.kind)}\${e.auto?' · auto':''}</span><span class="timeline-content">\${esc(e.from)} → \${esc(e.to)}\${e.taskId?\` · \${esc(e.taskId.slice(0,12))}\`:''}</span></div>\`;}
function render(snap){document.getElementById('stat-members').textContent=snap.members.length;document.getElementById('stat-tasks').textContent=snap.tasks.length;document.getElementById('stat-messages').textContent=snap.messages.length;document.getElementById('stat-time').textContent=new Date(snap.generatedAt).toLocaleTimeString();document.getElementById('badge-members').textContent=snap.members.length;document.getElementById('badge-tasks').textContent=snap.tasks.length;document.getElementById('badge-messages').textContent=snap.messages.length;const mg=document.getElementById('members-grid');if(snap.members.length>0){mg.innerHTML=snap.members.map(renderMemberCard).join('');}else{mg.innerHTML='<div class="empty-state"><div class="empty-icon">👥</div><div class="empty-text">暂无成员</div></div>';}const groups={queued:[],running:[],blocked:[],done:[],failed:[]};for(const t of snap.tasks){if(t.status==='queued'||t.status==='awaiting-approval')groups.queued.push(t);else if(t.status==='running'||t.status==='sending'||t.status==='pending')groups.running.push(t);else if(t.status==='blocked')groups.blocked.push(t);else if(t.status==='done')groups.done.push(t);else groups.failed.push(t);}const kb=document.getElementById('kanban-board');kb.innerHTML=[renderKanbanColumn('排队中','⏱️',groups.queued.slice(0,20),'queued'),renderKanbanColumn('进行中','⚡',groups.running.slice(0,20),'running'),renderKanbanColumn('阻塞','⚠️',groups.blocked.slice(0,20),'blocked'),renderKanbanColumn('已完成','✅',groups.done.slice(0,20),'done'),renderKanbanColumn('失败/取消','❌',groups.failed.slice(0,20),'failed')].join('');const tl=document.getElementById('timeline');if(snap.messages.length>0){tl.innerHTML=snap.messages.map(renderTimelineItem).join('');}else{tl.innerHTML='<div class="empty-state"><div class="empty-icon">📝</div><div class="empty-text">暂无消息</div></div>';}}
async function tick(){try{const response=await fetch('/session-hub/snapshot',{cache:'no-store'});if(!response.ok)throw new Error('HTTP '+response.status);const data=await response.json();render(data);}catch(error){console.error('Failed to fetch snapshot:',error);document.getElementById('stat-time').textContent='连接失败';}}
tick();setInterval(tick,2000);
</script>
</body>
</html>`;

  /** Serve the external board.html when present, so board styling can change without editing this plugin file. */
  function boardHtml() {
    try {
      return fs.readFileSync(path.join(PLUGIN_DIR, 'board.html'), 'utf8');
    } catch {
      return BOARD_HTML;
    }
  }

  function boardRequestAllowed(req) {
    const address = String(req?.socket?.remoteAddress ?? '');
    const plain = address.startsWith('::ffff:') ? address.slice(7) : address;
    if (!(plain === '::1' || plain.startsWith('127.'))) return false;
    const host = String(req?.headers?.host ?? '');
    if (host) {
      const name = host.split(':')[0].toLowerCase();
      if (!(name === 'localhost' || name === '127.0.0.1' || name === '::1')) return false;
    }
    return true;
  }

  function sendRes(res, status, contentType, body) {
    const payload = Buffer.isBuffer(body) ? body : Buffer.from(String(body), 'utf8');
    res.writeHead(status, {
      'content-type': contentType,
      'content-length': String(payload.byteLength),
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff',
    });
    res.end(payload);
  }

  try {
    ctx.inject(['webServer'], (scope) => {
      scope.effect(() => {
        const server = scope.webServer;
        if (!server || typeof server.register !== 'function') return undefined;
        const disposers = [];
        try {
          disposers.push(server.register({
            kind: 'exact',
            path: BOARD_PATH,
            handler: (req, res) => {
              if (!boardRequestAllowed(req)) return sendRes(res, 403, 'text/plain; charset=utf-8', 'forbidden');
              return sendRes(res, 200, 'text/html; charset=utf-8', boardHtml());
            },
          }));
          disposers.push(server.register({
            kind: 'exact',
            path: SNAPSHOT_PATH,
            handler: (req, res) => {
              if (!boardRequestAllowed(req)) return sendRes(res, 403, 'text/plain; charset=utf-8', 'forbidden');
              const requestUrl = new URL(req.url ?? '/', 'http://127.0.0.1');
              void buildSnapshot(requestUrl.searchParams.get('session') || undefined).then(
                (value) => sendRes(res, 200, 'application/json; charset=utf-8', JSON.stringify(value)),
                (error) => sendRes(res, 500, 'application/json; charset=utf-8', JSON.stringify({ ok: false, error: String(error?.message || error) })),
              );
            },
          }));
          disposers.push(server.register({
            kind: 'exact',
            path: '/session-hub/panel.js',
            handler: (req, res) => {
              if (!boardRequestAllowed(req)) return sendRes(res, 403, 'text/plain; charset=utf-8', 'forbidden');
              try {
                const body = fs.readFileSync(path.join(PLUGIN_DIR, 'panel.js'), 'utf8');
                return sendRes(res, 200, 'application/javascript; charset=utf-8', body);
              } catch (error) {
                return sendRes(res, 500, 'text/plain; charset=utf-8', `panel unavailable: ${String(error?.message || error)}`);
              }
            },
          }));
          disposers.push(server.register({
            kind: 'exact',
            path: '/session-hub/panel-config.json',
            handler: (req, res) => {
              if (!boardRequestAllowed(req)) return sendRes(res, 403, 'text/plain; charset=utf-8', 'forbidden');
              return sendRes(res, 200, 'application/json; charset=utf-8', JSON.stringify({
                boardUrl: `${boardOrigin()}/board`,
                version: 1,
              }));
            },
          }));
          disposers.push(server.register({
            kind: 'exact',
            path: '/session-hub/mentions.json',
            handler: (req, res) => {
              if (!boardRequestAllowed(req)) return sendRes(res, 403, 'text/plain; charset=utf-8', 'forbidden');
              const members = Object.entries(state.members).map(([sessionId, member]) => ({
                sessionId,
                alias: member.alias || Object.entries(state.aliases).find(([, id]) => id === sessionId)?.[0],
                title: sessionTitleCache.get(sessionId) || undefined,
              })).filter((member) => member.alias || member.title);
              return sendRes(res, 200, 'application/json; charset=utf-8', JSON.stringify({ members }));
            },
          }));
        } catch (error) {
          ctx.logger?.warn?.(`session-hub: board routes unavailable: ${String(error?.message || error)}`);
        }
        return () => {
          for (const dispose of disposers) {
            try {
              dispose?.();
            } catch (error) {
              ctx.logger?.info?.(`session-hub: board route teardown raced: ${safeErrorText(error)}`);
            }
          }
        };
      }, 'session-hub.board-routes');


    });
  } catch (error) {
    ctx.logger?.warn?.(`session-hub: web board not mounted: ${String(error?.message || error)}`);
  }

  startStandaloneBoard();

  ctx.logger?.info?.('session-hub: loaded (cross-session messaging + persistent task board + web board + standalone board origin)');
}



































