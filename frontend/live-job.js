(() => {
  if (location.protocol === 'file:') return;

  const time = new Intl.DateTimeFormat('zh-CN', {timeZone: 'Asia/Shanghai', timeStyle: 'medium', hourCycle: 'h23'});
  const sampleTime = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai', dateStyle: 'short', timeStyle: 'medium', hourCycle: 'h23',
  });
  const metrics = [
    {key: 'efficiency', stamp: 'cpu_observed_end', label: 'CPU 效率', color: '#8dbdff'},
    {key: 'memory_ratio', stamp: 'memory_observed_at', label: '内存效率', color: '#81d8bd', reason: 'memory_reason'},
  ];
  const percent = new Intl.NumberFormat('zh-CN', {style: 'percent', maximumFractionDigits: 1});
  let active;

  function element(tag, text, className) {
    const node = document.createElement(tag);
    node.textContent = text || '';
    if (className) node.className = className;
    return node;
  }

  function showSample(monitor) {
    const {canvas, pointer, plotted, tooltip, marker} = monitor;
    tooltip.hidden = marker.hidden = true;
    if (!pointer || !plotted.length) return;
    const rect = canvas.getBoundingClientRect();
    const x = pointer.clientX - rect.left, y = pointer.clientY - rect.top;
    const {left, right, top, bottom} = monitor.plotBounds;
    if (x < left - 6 || x > right + 6 || y < top - 6 || y > bottom + 6) return;

    let closest, distance = Infinity;
    for (const point of plotted) {
      const offset = Math.hypot(point.x - x, point.y - y);
      if (offset < distance) {
        closest = point;
        distance = offset;
      }
    }
    const heading = element('div', `${sampleTime.format(closest.time)}（北京时间）`);
    const readings = plotted.filter(point => point.time === closest.time).map(point => {
      const row = element('div', `${point.metric.label} ${percent.format(point.value)}`);
      row.style.color = point.metric.color;
      return row;
    });
    tooltip.replaceChildren(heading, ...readings);
    tooltip.hidden = marker.hidden = false;
    marker.style.left = `${closest.x}px`;
    marker.style.top = `${closest.y}px`;
    marker.style.color = closest.metric.color;
    const gap = 8;
    const tooltipX = Math.max(0, Math.min(closest.x + gap, rect.width - tooltip.offsetWidth));
    const above = closest.y - tooltip.offsetHeight - gap;
    const tooltipY = above >= 0 ? above : closest.y + gap;
    tooltip.style.left = `${tooltipX}px`;
    tooltip.style.top = `${tooltipY}px`;
  }

  function draw(monitor) {
    const canvas = monitor.canvas;
    const width = canvas.clientWidth, height = canvas.clientHeight;
    monitor.plotted = [];
    monitor.tooltip.hidden = monitor.marker.hidden = true;
    if (width < 120 || !height) return;
    const scale = window.devicePixelRatio || 1;
    canvas.width = Math.round(width * scale);
    canvas.height = Math.round(height * scale);
    const ctx = canvas.getContext('2d');
    ctx.scale(scale, scale);
    const left = 43, right = width - 14, top = 12, bottom = height - 25;
    monitor.plotBounds = {left, right, top, bottom};
    const points = monitor.series.flat();
    const end = points.length ? Math.max(...points.map(point => point.time)) : Date.now();
    const start = Math.min(end - 60000, ...points.map(point => point.time));
    const upper = Math.ceil(Math.max(1, ...points.map(point => point.value ?? 0)) * 4) / 4;
    const x = stamp => left + (stamp - start) / (end - start) * (right - left);
    const y = value => bottom - value / upper * (bottom - top);

    ctx.font = '11px system-ui, sans-serif';
    ctx.fillStyle = '#a6b7ca';
    ctx.strokeStyle = '#354154';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let index = 0; index <= 4; index++) {
      const value = upper * index / 4;
      ctx.beginPath();
      ctx.moveTo(left, y(value));
      ctx.lineTo(right, y(value));
      ctx.stroke();
      ctx.fillText(percent.format(value), left - 5, y(value));
    }
    if (!points.some(point => point.value !== null)) {
      ctx.textAlign = 'center';
      ctx.fillText('等待有效采样', (left + right) / 2, (top + bottom) / 2);
      return;
    }
    ctx.textAlign = 'left';
    ctx.fillText(time.format(start), left, height - 10);
    ctx.textAlign = 'right';
    ctx.fillText(time.format(end), right, height - 10);
    monitor.series.forEach((series, index) => {
      const metric = metrics[index];
      ctx.strokeStyle = ctx.fillStyle = metric.color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      let previous = null;
      for (const point of series) {
        if (point.value !== null) {
          if (previous && point.time - previous.time <= monitor.maxGap) ctx.lineTo(x(point.time), y(point.value));
          else ctx.moveTo(x(point.time), y(point.value));
        }
        previous = point.value === null ? null : point;
      }
      ctx.stroke();
      for (const point of series) {
        if (point.value === null) continue;
        const pointX = x(point.time), pointY = y(point.value);
        ctx.beginPath();
        ctx.arc(pointX, pointY, 2, 0, 2 * Math.PI);
        ctx.fill();
        monitor.plotted.push({...point, x: pointX, y: pointY, metric});
      }
    });
    showSample(monitor);
  }

  function stop(scope, message = '实时监控已停止。') {
    if (!active || (scope && !scope.contains(active.host))) return;
    const monitor = active;
    active = null;
    monitor.controller.abort();
    clearTimeout(monitor.timer);
    monitor.resize.disconnect();
    monitor.status.textContent = message;
    monitor.button.textContent = '开始实时监控';
  }

  function update(monitor, sample) {
    const job = sample.job;
    const checked = Date.parse(sample.queried_at);
    monitor.maxGap = sample.max_sample_age_seconds * 1000;
    monitor.started = Number.isFinite(Date.parse(job.started_at)) ? Date.parse(job.started_at) / 1000 : monitor.started;
    metrics.forEach((metric, index) => {
      const value = Number.isFinite(job[metric.key]) ? job[metric.key] : null;
      const stamp = Date.parse(job[metric.stamp]);
      const observed = value !== null && Number.isFinite(stamp);
      const series = monitor.series[index];
      const when = observed ? stamp : checked;
      if ((observed || series.at(-1)?.value != null) && !series.some(point => point.time === when)) {
        series.push({time: when, value: observed ? value : null});
        series.sort((a, b) => a.time - b.time);
        if (series.length > 60) series.shift();
      }
      monitor.values[index].textContent = observed ? percent.format(value) : job[metric.reason] ? '待核实' : '待采集';
      monitor.times[index].textContent = observed
        ? `采样于 ${time.format(stamp)}（${Math.max(0, Math.floor((checked - stamp) / 1000))} 秒前）`
        : job[metric.reason] || '';
    });
    monitor.status.textContent = [...sample.notes, job.reason].filter(Boolean).join(' ')
      || `${sample.source} · ${time.format(checked)} 检查 · 北京时间`;
    draw(monitor);
  }

  async function poll(monitor, signal) {
    let next = 15;
    try {
      const response = await fetch('/api/v1/live-job', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({job_id: monitor.id, started_at: monitor.started}),
        signal,
      });
      const sample = await response.json();
      if (signal.aborted) return;
      if (response.status === 409) {
        stop(monitor.host, '后台采集已开启。可重新开始单作业监控。');
        return;
      }
      if (response.status === 401 || response.status === 403) {
        stop(monitor.host, sample.message);
        return;
      }
      if (!response.ok) throw new Error(sample.message || '暂时无法读取实时数据。');
      if (sample.status === 'ended' || sample.status === 'unavailable') {
        stop(monitor.host, sample.message);
        return;
      }
      if (sample.job) update(monitor, sample);
      else monitor.status.textContent = sample.message;
      next = sample.next_poll_seconds || next;
    } catch (issue) {
      if (signal.aborted) return;
      monitor.status.textContent = `${issue.message} 将重试。`;
    }
    if (!signal.aborted) monitor.timer = setTimeout(() => poll(monitor, signal), next * 1000);
  }

  async function start(monitor) {
    stop();
    const controller = new AbortController();
    const {signal} = controller;
    active = monitor;
    monitor.controller = controller;
    monitor.resize.observe(monitor.canvas);
    monitor.button.textContent = '停止监控';
    monitor.status.textContent = '准备读取单作业监控数据…';
    try {
      const connection = await window.hpcCollection.getStatus();
      if (signal.aborted) return;
      if (connection.collection_enabled) {
        if (!await window.hpcCollection.confirmInterrupt(signal, {monitor: true})) {
          if (!signal.aborted) stop(monitor.host, '已取消单作业监控。');
          return;
        }
        await window.hpcCollection.setEnabled(false);
      }
      if (!signal.aborted) await poll(monitor, signal);
    } catch (issue) {
      if (!signal.aborted) stop(monitor.host, issue.message);
    }
  }

  function open(job, host) {
    const id = String(job.job_id ?? job.id);
    if (active?.host === host && active.id === id) return;
    stop();
    const card = element('section', null, 'live-job-card');
    const title = element('h3', '单作业实时监控');
    const button = element('button', '', 'refresh');
    button.type = 'button';
    const header = element('div', null, 'live-job-heading');
    header.append(title, button);
    const readings = element('div', null, 'live-job-readings');
    const values = [], times = [];
    metrics.forEach(metric => {
      const reading = element('div', null, 'live-job-reading');
      reading.style.setProperty('--metric-color', metric.color);
      const value = element('strong', '待采集');
      const observed = element('small');
      reading.append(element('span', metric.label), value, observed);
      readings.append(reading);
      values.push(value);
      times.push(observed);
    });
    const status = element('p', '', 'live-job-status');
    status.setAttribute('role', 'status');
    const canvas = document.createElement('canvas');
    canvas.setAttribute('role', 'img');
    canvas.setAttribute('aria-label', `作业 ${id} 的 CPU 和内存效率时间曲线`);
    const plot = element('div', null, 'live-job-plot');
    const marker = element('span', null, 'live-job-marker');
    marker.setAttribute('aria-hidden', 'true');
    const tooltip = element('div', null, 'live-job-tooltip');
    marker.hidden = tooltip.hidden = true;
    plot.append(canvas, marker, tooltip);
    const started = Date.parse(job.started_at) / 1000;
    const monitor = {host, id, started: Number.isFinite(started) ? started : null,
                     values, times, status, button, canvas, marker, tooltip,
                     series: [[], []], plotted: [], maxGap: 60000};
    canvas.addEventListener('pointermove', event => {
      monitor.pointer = {clientX: event.clientX, clientY: event.clientY};
      showSample(monitor);
    });
    canvas.addEventListener('pointerleave', () => {
      monitor.pointer = null;
      showSample(monitor);
    });
    monitor.resize = new ResizeObserver(() => draw(monitor));
    button.addEventListener('click', () => {
      if (active === monitor) stop(host);
      else start(monitor);
    });
    card.append(header, readings, plot, status);
    host.replaceChildren(card);
    start(monitor);
  }

  document.addEventListener('hpc-collection-changed', event => {
    if (event.detail?.enabled) stop(undefined, '后台采集已开启，单作业监控已停止。');
  });
  document.addEventListener('hpc-source-changed', () => stop());
  window.hpcLiveJob = {open, stop};
})();
