const $ = (selector) => document.querySelector(selector);
const state = {view: 'running', metric: 'cpu', data: null, selected: null, loading: false, request: 0, events: null, pendingRevision: -1, autoRequest: null};
const elements = {
  grid: $('#grid'), notice: $('#notice'), search: $('#search'), partition: $('#partition'), drawer: $('#drawer'),
  sortField: $('#sort-field'), sortOrder: $('#sort-order'), autoRefresh: $('#auto-refresh'),
};
const date = (value) => value ? new Date(value).toLocaleString('zh-CN', {hour12: false}) : '未提供';
const duration = (seconds) => seconds == null ? '未提供' : `${Math.floor(seconds / 3600)} 小时 ${Math.floor(seconds % 3600 / 60)} 分钟 ${seconds % 60} 秒`;
const gib = (bytes) => bytes == null ? '未提供' : `${(bytes / (1024 ** 3)).toLocaleString('zh-CN', {maximumFractionDigits: 2})} GiB`;
const percent = (ratio) => Number.isFinite(ratio) ? `${(ratio * 100).toFixed(1)}%` : '暂无可信效率';
const cores = (value) => value == null ? '未提供' : `${value.toLocaleString('zh-CN')} 核`;
const jobDuration = (job) => job.state === 'RUNNING' ? job.active_seconds : job.elapsed_seconds;
const metricValue = (job) => state.metric === 'cpu' ? job.efficiency : job.memory_ratio;
const metricLabel = () => state.metric === 'cpu' ? 'CPU 效率' : '内存效率';
const jobIds = new Intl.Collator('zh-CN', {numeric: true});
const sortValues = {
  efficiency: metricValue,
  'cpu-time': (job) => job.cpu_seconds,
  duration: jobDuration,
  'cpu-cores': (job) => job.cpus,
  'memory-used': (job) => job.memory_used_bytes,
};

function detail(job) {
  state.selected = job.id;
  $('#detail-title').textContent = `作业 ${job.id}`;
  const running = job.state === 'RUNNING';
  const memoryLabel = running ? '实测内存用量' : job.memory_basis === 'concurrent_peak' ? '作业级内存峰值' : 'seff 内存估算';
  const rows = [
    ['状态', running ? '运行中' : '已完成'], ['分区', job.partition], ['分配 CPU 核数', cores(job.cpus)],
    ['申请 CPU 核数', cores(job.requested_cpus)], ['开始时间', date(job.started_at)], ['结束时间', date(job.ended_at)],
    ['运行时长', duration(jobDuration(job))],
    [running ? '窗口 CPU 时间' : 'CPU 时间', job.cpu_seconds == null ? '未提供' : `${job.cpu_seconds.toLocaleString()} CPU 秒`],
    [running ? '窗口 CPU 效率' : 'CPU 效率', percent(job.efficiency)],
    ['内存效率', percent(job.memory_ratio)], [memoryLabel, gib(job.memory_used_bytes)],
    ['内存分配量', gib(job.memory_allocated_bytes)], ['内存申请量', gib(job.requested_memory_bytes)],
  ];
  if (running) {
    rows.push(['分配节点', job.nodes?.join('、') || '未提供'],
      ['CPU 实测窗口', job.cpu_observed_end ? `${date(job.cpu_observed_start)} 至 ${date(job.cpu_observed_end)}` : '待采集'],
      ['内存采样时间', date(job.memory_observed_at)]);
  }
  const container = $('#details'); container.replaceChildren();
  for (const [label, value] of rows) {
    const row = document.createElement('div'); row.className = 'detail-row';
    const name = document.createElement('span'); name.textContent = label;
    const content = document.createElement('strong'); content.textContent = value;
    row.append(name, content); container.append(row);
  }
  const note = document.createElement('div'); note.className = 'detail-reason';
  note.textContent = job.reason || (running ? 'CPU 效率按各分配节点共同覆盖的实测窗口计算。' : '整段运行的平均 CPU 效率。该数值描述 CPU 使用情况，需结合任务特点进一步判断。');
  container.append(note); elements.drawer.hidden = false; renderGrid();
}

function compareJobs(first, second) {
  const field = elements.sortField.value;
  const order = elements.sortOrder.value === 'desc' ? -1 : 1;
  const byId = jobIds.compare(first.id, second.id);
  if (field === 'job-id') return order * byId;
  const value = sortValues[field];
  const a = value(first), b = value(second);
  const hasA = Number.isFinite(a), hasB = Number.isFinite(b);
  if (hasA !== hasB) return hasA ? -1 : 1;
  return (hasA ? order * (a - b) : 0) || byId;
}

function renderGrid() {
  const search = elements.search.value.trim(), partition = elements.partition.value;
  const jobs = (state.data?.jobs || []).filter(job => job.id.includes(search) && (!partition || job.partition === partition));
  if (elements.sortField.value !== 'source') jobs.sort(compareJobs);
  elements.grid.replaceChildren();
  for (const job of jobs) {
    const button = document.createElement('button');
    const ratio = metricValue(job);
    const level = Number.isFinite(ratio) ? `level-${Math.max(0, Math.min(4, Math.floor(ratio * 5)))}` : 'unknown';
    button.className = `cell ${level}${state.selected === job.id ? ' selected' : ''}`;
    const observedAt = state.metric === 'cpu' ? job.cpu_observed_end : job.memory_observed_at;
    const observed = observedAt ? ` · 采样截至 ${date(observedAt)}` : '';
    button.title = `作业 ${job.id} · ${metricLabel()} ${percent(ratio)}${observed}`;
    button.setAttribute('aria-label', button.title); button.addEventListener('click', () => detail(job)); elements.grid.append(button);
  }
  $('#plot-count').textContent = state.data?.source_kind === 'disconnected'
    ? '作业列表' : `匹配 ${jobs.length.toLocaleString()} 个作业`;
}

function renderMetricSummary() {
  $('#available-label').textContent = `${metricLabel()}可计算`;
  $('#metric-footnote').textContent = state.metric === 'cpu'
    ? '运行中作业使用实测时间窗内的 CPU 用量。已完成作业使用整段运行的 CPU 记账数据。'
    : '内存效率按用量除以分配量。运行中作业使用对齐后的采样值。已完成作业使用 seff 估算或已核实的作业级峰值。';
  const data = state.data;
  const disconnected = !data || data.source_kind === 'disconnected';
  const available = data?.jobs.filter(job => Number.isFinite(metricValue(job))).length ?? 0;
  $('#available').textContent = disconnected ? '—' : available.toLocaleString('zh-CN');
  if (!data) return;
  let message = data.jobs.length ? '深蓝表示效率低，亮蓝表示效率高。' : '本次快照没有符合条件的作业。';
  if (state.view === 'running' && data.coverage) {
    const coverage = data.coverage;
    message = `运行作业 ${coverage.catalog_count} 条 · ${metricLabel()}可计算 ${available} 条 · 本轮读取节点 ${coverage.nodes_visited}/${coverage.nodes_total}`;
    if (data.updates_on_completion) {
      message += ` · 本轮耗时 ${duration(Math.round(data.collection_seconds))} · 指标时间以各作业采样为准`;
      if (coverage.nodes_attempted < coverage.nodes_total) message += ` · 本轮提前结束，已尝试 ${coverage.nodes_attempted} 台节点`;
    }
    for (const error of Object.values(coverage.errors || {})) message += ` · Prometheus ${error}`;
    const gap = Object.entries(coverage.cpu_gaps || {}).sort((a, b) => b[1] - a[1])[0];
    if (!available && state.metric === 'cpu' && gap) message += ` · ${gap[0]}`;
    if (!available && state.metric === 'memory') message += ' · 暂无满足计算条件的内存采样与分配记录。';
  }
  if (data.status === 'warming_up') message = data.updates_on_completion ? '后台正在进行首轮采集。' : '后台正在获取第一批作业与监控样本。';
  if (data.status === 'stale') message = `当前展示已过期快照。 ${message}`;
  if (data.status === 'unavailable') message = data.error?.message || '暂无可用快照。';
  if (data.catalog_error) message += ` · Slurm 作业列表刷新失败。 ${data.catalog_error.message}`;
  if (data.source_kind === 'synthetic') message = `合成演示数据。 ${message} 查看真实作业请打开普通 PowerShell 中启动的服务网址。`;
  elements.notice.textContent = disconnected ? '请点击右上角的「连接 SJTU HPC」查看作业。' : message;
}

function updateRefreshHint() {
  $('#interval').textContent = !elements.autoRefresh.checked ? '已暂停'
    : state.data?.updates_on_completion ? '整轮采集完成时' : '每 10 秒';
}

function syncLiveUpdates(enabled) {
  if (!enabled || !elements.autoRefresh.checked || state.view !== 'running') {
    state.events?.close();
    state.events = null;
    state.pendingRevision = -1;
    return;
  }
  if (state.events || typeof EventSource === 'undefined') return;
  const events = new EventSource('/api/v1/live-updates');
  state.events = events;
  events.onmessage = event => {
    if (state.events !== events || $('#home-view').hidden) return;
    state.pendingRevision = Number(event.data);
    if (state.pendingRevision > (state.data?.collection_revision ?? -1)) load(false, true);
  };
  events.addEventListener('stopped', () => {
    if (state.events !== events) return;
    syncLiveUpdates(false);
    load(false, true);
  });
}

async function load(refresh = false, automatic = false) {
  if (state.loading || $('#home-view').hidden || (automatic && !elements.autoRefresh.checked)) return;
  state.loading = true; const request = ++state.request; const view = state.view;
  const controller = automatic ? new AbortController() : null;
  state.autoRequest = controller;
  $('#refresh').disabled = true;
  if (!state.data || refresh) { elements.notice.className = 'notice'; elements.notice.textContent = '正在获取数据…'; }
  try {
    const response = await fetch(`/api/v1/job-heatmap?view=${view}${refresh ? '&refresh=1' : ''}`, {cache: 'no-store', signal: controller?.signal});
    const data = await response.json(); if (!response.ok) throw new Error(data.error || '数据获取失败');
    if (request !== state.request || controller?.signal.aborted) return;
    state.data = data;
    elements.notice.className = 'notice';
    syncLiveUpdates(data.updates_on_completion);
    updateRefreshHint();
    const synthetic = data.source_kind === 'synthetic';
    const disconnected = data.source_kind === 'disconnected';
    $('#source-label').textContent = synthetic ? '合成演示数据' : disconnected ? '未连接' : '交大超算';
    $('#source-kind').textContent = synthetic ? '合成数据' : disconnected ? '等待连接 SJTU HPC'
      : view === 'running' && data.source === 'Slurm + Prometheus' ? 'SLURM + PROMETHEUS' : 'SLURM ACCOUNTING';
    const selected = data.jobs.find(job => job.id === state.selected);
    if (state.selected && !selected) { state.selected = null; elements.drawer.hidden = true; }
    const previous = elements.partition.value; elements.partition.replaceChildren(new Option('全部分区', ''));
    for (const name of [...new Set(data.jobs.map(job => job.partition))].sort()) elements.partition.add(new Option(name, name));
    if ([...elements.partition.options].some(option => option.value === previous)) elements.partition.value = previous;
    $('#total').textContent = disconnected ? '—' : data.jobs.length.toLocaleString();
    $('#updated').textContent = date(data.queried_at); $('#source-time').textContent = data.source_updated_at ? `源数据更新 ${date(data.source_updated_at)}` : '源更新时间未提供';
    renderMetricSummary();
    if (selected) detail(selected); else renderGrid();
  } catch (error) {
    if (request === state.request && !controller?.signal.aborted) { elements.notice.className = 'notice error'; elements.notice.textContent = `${error.message || '连接失败'}。${state.data ? '当前保留上次结果，请留意查询时间。' : '请稍后重试。'}`; }
  } finally {
    if (request === state.request) {
      state.loading = false;
      state.autoRequest = null;
      $('#refresh').disabled = false;
      if (state.pendingRevision > (state.data?.collection_revision ?? -1)) {
        state.pendingRevision = -1;
        load(false, true);
      }
    }
  }
}

if (location.protocol === 'file:') {
  $('#local-file-notice').hidden = false;
  $('#source-label').textContent = '本地文件';
  $('#source-kind').textContent = '请通过本地服务访问';
  elements.notice.hidden = true;
  for (const control of document.querySelectorAll('#home-prompt input, #home-prompt button, #show-conversation, #hpc-connect-button, .tab, .toolbar input, .toolbar select, #refresh')) {
    control.disabled = true;
  }
} else {
  const viewTabs = document.querySelectorAll('[data-view]');
  const metricTabs = document.querySelectorAll('[data-metric]');
  viewTabs.forEach(button => button.addEventListener('click', () => {
    if (state.view === button.dataset.view) return;
    syncLiveUpdates(false);
    state.autoRequest?.abort();
    state.request++; state.loading = false; state.view = button.dataset.view; state.data = null; state.selected = null; elements.drawer.hidden = true;
    viewTabs.forEach(tab => {
      tab.classList.toggle('active', tab === button);
      tab.setAttribute('aria-pressed', String(tab === button));
    });
    $('#view-description').textContent = state.view === 'completed' ? '最近 5 分钟完成的作业 · 每个格子代表一个作业' : '后台采集的运行作业 · 每个格子代表一个作业';
    updateRefreshHint();
    $('#total').textContent = '—'; $('#available').textContent = '—'; $('#updated').textContent = '—'; $('#source-time').textContent = '等待数据'; renderGrid(); load();
  }));
  metricTabs.forEach(button => button.addEventListener('click', () => {
    state.metric = button.dataset.metric;
    metricTabs.forEach(tab => {
      tab.classList.toggle('active', tab === button);
      tab.setAttribute('aria-pressed', String(tab === button));
    });
    renderMetricSummary();
    renderGrid();
  }));
  elements.search.addEventListener('input', renderGrid);
  elements.partition.addEventListener('change', renderGrid);
  elements.sortField.addEventListener('change', () => {
    elements.sortOrder.disabled = elements.sortField.value === 'source';
    renderGrid();
  });
  elements.sortOrder.addEventListener('change', renderGrid);
  elements.autoRefresh.addEventListener('change', () => {
    updateRefreshHint();
    if (elements.autoRefresh.checked) {
      syncLiveUpdates(state.data?.updates_on_completion);
      load(false, true);
    } else {
      syncLiveUpdates(false);
      state.autoRequest?.abort();
    }
  });
  $('#refresh').addEventListener('click', () => load(true));
  $('#close').addEventListener('click', () => {elements.drawer.hidden = true; state.selected = null; renderGrid();});
  setInterval(() => {
    if (!state.events || state.events.readyState !== EventSource.OPEN) load(false, true);
  }, 10000);
  document.addEventListener('heatmap-restored', () => load(false, Boolean(state.data)));
  document.addEventListener('hpc-source-changed', () => {
    syncLiveUpdates(false);
    state.autoRequest?.abort();
    state.request++;
    state.loading = false;
    state.data = null;
    state.selected = null;
    elements.drawer.hidden = true;
    elements.partition.replaceChildren(new Option('全部分区', ''));
    $('#total').textContent = '—'; $('#available').textContent = '—'; $('#updated').textContent = '—';
    $('#source-time').textContent = '等待数据';
    updateRefreshHint();
    renderGrid();
    load();
  });
  load();
}
