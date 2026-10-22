/* Session Hub native client plugin: a real right-sidebar tab type for this session's board. */
window.__ModuleLoader__.load({
  id: 'dsh-crosssession',
  factory: (require) => {
    var module = { exports: {} };
    var exports = module.exports;
    Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });

    const react = require('react');
    const h = react.createElement;

    const TAB_ID = 'dsh-crosssession/board';
    const TAB_KIND = 'sessionHubBoard';
    const PANEL_CONFIG_PATH = '/session-hub/panel-config.json';
    const MENTIONS_PATH = '/session-hub/mentions.json';
    const OPEN_ONCE_KEY = 'dsh-crosssession.right-sidebar-opened-v3';

    /** Cached member names for the `@` mention source (alias + title). */
    const mentions = { members: [], aliases: [] };

    function refreshMentions() {
      return fetch(MENTIONS_PATH, { cache: 'no-store' })
        .then((response) => (response.ok ? response.json() : Promise.reject(new Error('HTTP ' + response.status))))
        .then((data) => {
          const list = Array.isArray(data?.members) ? data.members : [];
          mentions.members = list;
          mentions.aliases = list
            .map((member) => member.alias)
            .filter((alias) => typeof alias === 'string' && alias.length > 0);
        })
        .catch(() => { /* keep the previous cache */ });
    }

    /** Register the `@` source so members show up as candidates and render as editable chips. */
    function registerMentionSource(ctx) {
      const inputTriggers = ctx.get?.('inputTriggers');
      if (!inputTriggers || typeof inputTriggers.registerSource !== 'function') return undefined;
      return ctx.effect(() => inputTriggers.registerSource({
        trigger: '@',
        name: 'session-hub',
        showGroupTitle: false,
        async candidates(_session, request) {
          await refreshMentions();
          const query = String(request?.query ?? '').toLowerCase();
          const items = [];
          for (const member of mentions.members) {
            const alias = typeof member.alias === 'string' ? member.alias : '';
            const title = typeof member.title === 'string' ? member.title : '';
            const name = alias || title;
            if (!name) continue;
            if (query && !name.toLowerCase().includes(query) && !title.toLowerCase().includes(query)) continue;
            items.push({
              name,
              ...(alias && title ? { label: '@' + alias + ' · ' + title } : {}),
              description: '发送消息 / 任务到该会话',
              value: 'session-hub:' + String(member.sessionId || ''),
            });
          }
          return items.slice(0, 8);
        },
        onPick({ candidate }) {
          const mention = String(candidate?.name || '').replace(/^@/, '');
          if (!mention) return { text: '@' };
          return { text: '@' + mention };
        },
        lexicon() {
          return mentions.aliases.slice();
        },
      }), 'session-hub: @ mentions');
    }

    /** Which session the main view currently retains (the board is scoped to it). */
    function readCurrentSessionId(ctx) {
      try {
        const state = ctx.sessions?.list?.getSnapshot?.();
        const byId = state?.byId || {};
        for (const id of Object.keys(byId)) {
          const retained = Number(ctx.sessions.retainInfo(id).getSnapshot()?.retainedBy?.mainView ?? 0);
          if (retained > 0) return id;
        }
        return undefined;
      } catch {
        return undefined;
      }
    }

    function useCurrentSessionId(ctx) {
      const [sessionId, setSessionId] = react.useState(() => readCurrentSessionId(ctx));
      react.useEffect(() => {
        let stop;
        try {
          stop = ctx.sessions.list.subscribe(() => setSessionId(readCurrentSessionId(ctx)));
        } catch {
          /* sessions store unavailable */
        }
        return () => { try { stop?.(); } catch { /* ignore */ } };
      }, []);
      return sessionId;
    }

    /** Read the current standalone board URL (a different loopback origin the tab can frame). */
    async function fetchBoardUrl() {
      const response = await fetch(PANEL_CONFIG_PATH, { cache: 'no-store' });
      if (!response.ok) throw new Error('panel config HTTP ' + response.status);
      const config = await response.json();
      if (typeof config?.boardUrl !== 'string' || !config.boardUrl) throw new Error('panel config has no boardUrl');
      return config.boardUrl;
    }

    function openBoardTab(ctx) {
      try {
        ctx.sidebarRight.openTab(TAB_KIND, { params: {} });
        return true;
      } catch (error) {
        ctx.logger?.warn?.('session-hub client: could not open the board tab: ' + String(error?.message || error));
        return false;
      }
    }

    function BoardTab() {
      const ctx = BoardTab.ctx;
      const sessionId = useCurrentSessionId(ctx);
      const [url, setUrl] = react.useState(null);
      const [error, setError] = react.useState(null);

      react.useEffect(() => {
        let live = true;
        fetchBoardUrl().then(
          (value) => { if (live) setUrl(value); },
          (reason) => { if (live) setError(String(reason?.message || reason)); },
        );
        return () => { live = false; };
      }, []);

      if (error) {
        return h('div', { style: { padding: '16px', fontSize: '12px', color: 'var(--dsw-alias-status-danger, #dc2626)' } }, '任务板加载失败：' + error);
      }
      if (!url) {
        return h('div', { style: { padding: '16px', fontSize: '12px', color: 'var(--dsw-alias-label-tertiary, #9aa1ab)' } }, '加载中…');
      }
      const src = url + (sessionId ? '?session=' + encodeURIComponent(sessionId) : '');
      return h('iframe', {
        key: src,
        src,
        title: 'Session Hub 任务板',
        style: { width: '100%', height: '100%', border: 0, display: 'block', background: 'transparent' },
      });
    }

    function BoardTabTitle() {
      return h('span', null, '任务板');
    }

    const inject = ['sidebarRight', 'sidebarRightTabs', 'slots', 'sessions', 'inputTriggers'];

    function apply(ctx) {
      ctx.logger?.info?.('session-hub client: loaded');
      BoardTab.ctx = ctx;

      registerMentionSource(ctx);
      ctx.effect(() => {
        void refreshMentions();
        const timer = window.setInterval(() => { void refreshMentions(); }, 30_000);
        return () => window.clearInterval(timer);
      }, 'session-hub: mention cache');

      ctx.effect(() => ctx.sidebarRightTabs.register({
        id: TAB_ID,
        kind: TAB_KIND,
        priority: 'builtin',
        title: () => '任务板',
      }), 'session-hub: board tab type');

      ctx.slots.inject('sidebar.right.pane.tab', () => ctx.slots.register({
        name: 'sidebar.right.pane.tab',
        key: TAB_ID,
      }, BoardTab));

      ctx.slots.inject('sidebar.right.pane.tab.title', () => ctx.slots.register({
        name: 'sidebar.right.pane.tab.title',
        key: TAB_ID,
      }, BoardTabTitle));

      ctx.slots.inject('conversation.session.header.utilities', () => ctx.slots.register({
        name: 'conversation.session.header.utilities',
        id: 'session-hub-board',
        order: 40,
        inject: () => ({}),
      }, function SessionHubBoardAction() {
        return h('button', {
          type: 'button',
          title: '在右侧栏打开本会话的 Session Hub 任务板',
          'aria-label': 'Session Hub 任务板',
          onClick: () => openBoardTab(ctx),
          style: {
            appearance: 'none',
            border: '1px solid var(--dsw-alias-border-l1, #e6e8eb)',
            background: 'transparent',
            color: 'var(--dsw-alias-label-secondary, #6b7280)',
            borderRadius: '6px',
            padding: '2px 8px',
            fontSize: '12px',
            lineHeight: '18px',
            cursor: 'pointer',
          },
        }, '任务板');
      }));

      // Open once per browser session so the board is discoverable.
      try {
        if (window.sessionStorage.getItem(OPEN_ONCE_KEY) !== '1') {
          window.sessionStorage.setItem(OPEN_ONCE_KEY, '1');
          window.setTimeout(() => openBoardTab(ctx), 800);
        }
      } catch {
        /* private mode: skip the automatic open */
      }
    }

    exports.apply = apply;
    exports.inject = inject;
    return module.exports;
  },
});
