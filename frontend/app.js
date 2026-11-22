const $ = (selector) => document.querySelector(selector);
const state = {view: 'completed', data: null, selected: null, loading: false, request: 0};
const elements = {grid: $('#grid'), notice: $('#notice'), search: $('#search'), partition: $('#partition'), drawer: $('#drawer')};
const date = (value) => value ? new Date(value).toLocaleString('zh-CN', {hour12: false}) : '未提供';
const duration = (seconds) => seconds == null ? '未提供' : `${Math.floor(seconds / 3600)} 小时 ${Math.floor(seconds % 3600 / 60)} 分钟 ${seconds % 60} 秒`;

function detail(job) {
  state.selected = job.id;
  $('#detail-title').textContent = `作业 ${job.id}`;
  const rows = [['状态', job.state === 'RUNNING' ? '运行中' : '已完成'], ['分区', job.partition], ['分配 CPU', job.cpus == null ? '未提供' : `${job.cpus} 个`], ['开始时间', date(job.started_at)], ['结束时间', date(job.ended_at)], ['运行时长', duration(job.elapsed_seconds)], ['CPU 时间', job.cpu_seconds == null ? '未提供' : `${job.cpu_seconds.toLocaleString()} CPU 秒`], ['CPU 效率', job.efficiency == null ? '暂无可信效率' : `${(job.efficiency * 100).toFixed(1)}%`]];
  const container = $('#details'); container.replaceChildren();
  for (const [label, value] of rows) {
    const row = document.createElement('div'); row.className = 'detail-row';
    const name = document.createElement('span'); name.textContent = label;
    const content = document.createElement('strong'); content.textContent = value;
    row.append(name, content); container.append(row);
  }
  const note = document.createElement('div'); note.className = 'detail-reason';
  note.textContent = job.reason || '整段运行的平均 CPU 效率。该数值描述 CPU 使用情况，需结合任务特点进一步判断。';
  container.append(note); elements.drawer.hidden = false; renderGrid();
}

function renderGrid() {
  const jobs = (state.data?.jobs || []).filter(job => job.id.includes(elements.search.value.trim()) && (!elements.partition.value || job.partition === elements.partition.value));
  elements.grid.replaceChildren();
  for (const job of jobs) {
    const button = document.createElement('button');
    const level = job.efficiency == null ? 'unknown' : `level-${Math.min(4, Math.floor(job.efficiency * 5))}`;
    button.className = `cell ${level}${state.selected === job.id ? ' selected' : ''}`;
    button.title = `作业 ${job.id} · ${job.efficiency == null ? '暂无可信效率' : (job.efficiency * 100).toFixed(1) + '%'}`;
    button.setAttribute('aria-label', button.title); button.addEventListener('click', () => detail(job)); elements.grid.append(button);
  }
  $('#plot-count').textContent = `匹配 ${jobs.length.toLocaleString()} 个作业`;
}

async function load() {
  if (state.loading) return;
  state.loading = true; const request = ++state.request; const view = state.view;
  $('#refresh').disabled = true; elements.notice.className = 'notice'; elements.notice.textContent = '正在获取数据…';
  try {
    const response = await fetch(`/api/v1/job-heatmap?view=${view}`, {cache: 'no-store'});
    const data = await response.json(); if (!response.ok) throw new Error(data.error || '数据获取失败');
    if (request !== state.request) return;
    state.data = data;
    const selected = data.jobs.find(job => job.id === state.selected);
    if (state.selected && !selected) { state.selected = null; elements.drawer.hidden = true; }
    const previous = elements.partition.value; elements.partition.replaceChildren(new Option('全部分区', ''));
    for (const name of [...new Set(data.jobs.map(job => job.partition))].sort()) elements.partition.add(new Option(name, name));
    if ([...elements.partition.options].some(option => option.value === previous)) elements.partition.value = previous;
    $('#total').textContent = data.jobs.length.toLocaleString(); $('#available').textContent = data.jobs.filter(job => job.efficiency != null).length.toLocaleString();
    $('#updated').textContent = date(data.queried_at); $('#source-time').textContent = data.source_updated_at ? `源数据更新 ${date(data.source_updated_at)}` : '源更新时间未提供';
    elements.notice.textContent = data.jobs.length ? (view === 'running' ? '运行中作业的效率暂不可用，灰色方格代表暂无可信数据。' : '深蓝表示效率低，亮蓝表示效率高。') : '当前时间窗口内没有符合条件的作业。';
    if (selected) detail(selected); else renderGrid();
  } catch (error) {
    if (request === state.request) { elements.notice.className = 'notice error'; elements.notice.textContent = `${error.message || '连接失败'}。${state.data ? '当前保留上次结果，请留意查询时间。' : '请稍后重试。'}`; }
  } finally { if (request === state.request) {state.loading = false; $('#refresh').disabled = false;} }
}

document.querySelectorAll('.tab').forEach(button => button.addEventListener('click', () => {
  if (state.view === button.dataset.view) return;
  state.request++; state.loading = false; state.view = button.dataset.view; state.data = null; elements.drawer.hidden = true;
  document.querySelectorAll('.tab').forEach(tab => tab.classList.toggle('active', tab === button));
  $('#view-description').textContent = state.view === 'completed' ? '最近 5 分钟完成的作业 · 每个格子代表一个作业' : '当前运行作业快照 · 每个格子代表一个作业';
  $('#interval').textContent = '10 秒';
  $('#total').textContent = '—'; $('#available').textContent = '—'; $('#updated').textContent = '—'; $('#source-time').textContent = '等待数据'; renderGrid(); load();
}));
elements.search.addEventListener('input', renderGrid);
elements.partition.addEventListener('change', renderGrid);
$('#refresh').addEventListener('click', load);
$('#close').addEventListener('click', () => {elements.drawer.hidden = true; state.selected = null; renderGrid();});
setInterval(load, 10000);
load();
