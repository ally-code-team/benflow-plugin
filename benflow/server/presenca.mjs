// Benflow: ganchos de presença do terminal (arquivo gerado por npm run build:connector; não edite).

// connector/presenca.ts
import { execFile } from "node:child_process";

// connector/presence.ts
import { createHash } from "node:crypto";
import { mkdirSync as mkdirSync3, readdirSync, readFileSync as readFileSync4, rmSync as rmSync2, statSync as statSync4, writeFileSync as writeFileSync3 } from "node:fs";
import os2 from "node:os";
import { basename as basename2, dirname as dirname3, join as join4 } from "node:path";

// connector/client.ts
import { createWriteStream, openAsBlob, statSync } from "node:fs";
import { rm } from "node:fs/promises";
import { basename, extname, join } from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
var SUPPORTED_JOB_TYPES = ["executar_chamado", "publicar", "continuar", "conversa", "sugestoes", "atualizar_cofre", "gravar_video", "testar"];
var JOB_REQUEST_CAPS = ["conversa-painel", "paralelo", "orquestrador", "sugestoes-arquivos"];
var JOB_HEADER = "x-benflow-job";
var WORK_JOB_HEADER = "x-benflow-trabalho";
var ApiError = class extends Error {
  constructor(message, status, retryable) {
    super(message);
    this.status = status;
    this.retryable = retryable;
    this.name = "ApiError";
  }
  status;
  retryable;
};
var MAX_LOG_LINES = 50;
var MAX_LOG_TEXT = 2e3;
var MAX_DOWNLOAD_BYTES = 200 * 1024 * 1024;
var MIME = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".pdf": "application/pdf",
  ".txt": "text/plain",
  ".log": "text/plain",
  ".md": "text/markdown",
  ".json": "application/json",
  ".html": "text/html",
  ".csv": "text/csv",
  ".mp4": "video/mp4",
  ".webm": "video/webm",
  ".zip": "application/zip"
};
function guessMime(path) {
  return MIME[extname(path).toLowerCase()] ?? "application/octet-stream";
}
var defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));
function errText(err) {
  if (err instanceof Error) {
    const cause = err.cause;
    const code = cause && typeof cause === "object" && "code" in cause ? String(cause.code) : "";
    return code ? `${err.message} (${code})` : err.message;
  }
  return String(err);
}
function fileNameFromDisposition(header, fallback) {
  let name = "";
  if (header) {
    const star = /filename\*\s*=\s*(?:UTF-8|utf-8)''([^;]+)/.exec(header);
    const plain = /filename\s*=\s*"([^"]*)"|filename\s*=\s*([^;]+)/.exec(header);
    try {
      if (star) name = decodeURIComponent(star[1].trim());
    } catch {
      name = "";
    }
    if (!name && plain) name = (plain[1] ?? plain[2] ?? "").trim();
  }
  name = basename(name.replace(/\\/g, "/")).replace(/[\u0000-\u001f\u007f/\\:*?"<>|]/g, "_").trim();
  if (!name || name === "." || name === "..") name = fallback;
  return name.slice(0, 150);
}
var AgentClient = class _AgentClient {
  baseUrl;
  token;
  fetchFn;
  timeoutMs;
  retries;
  retryBaseMs;
  maxRetryDelayMs;
  sleep;
  userAgent;
  jobId;
  workJobId;
  opts;
  constructor(opts) {
    this.opts = opts;
    this.baseUrl = opts.url.replace(/\/+$/, "");
    this.token = opts.token;
    this.fetchFn = opts.fetch ?? ((input, init) => fetch(input, init));
    this.timeoutMs = opts.timeoutMs ?? 3e4;
    this.retries = opts.retries ?? 3;
    this.retryBaseMs = opts.retryBaseMs ?? 1e3;
    this.maxRetryDelayMs = opts.maxRetryDelayMs ?? 3e4;
    this.sleep = opts.sleep ?? defaultSleep;
    this.userAgent = opts.userAgent ?? "benflow-conector";
    this.jobId = typeof opts.jobId === "number" && Number.isInteger(opts.jobId) && opts.jobId > 0 ? opts.jobId : null;
    this.workJobId = typeof opts.workJobId === "number" && Number.isInteger(opts.workJobId) && opts.workJobId > 0 ? opts.workJobId : null;
  }
  // O mesmo cliente preso a um trabalho da conversa pelo painel: toda requisição dele leva o X-Benflow-Job.
  withJob(jobId) {
    return new _AgentClient({ ...this.opts, jobId });
  }
  // O mesmo cliente preso ao trabalho de um card ou da conversa do Telegram: toda requisição leva o X-Benflow-Trabalho.
  withWorkJob(workJobId) {
    return new _AgentClient({ ...this.opts, workJobId });
  }
  buildUrl(path, query) {
    const url = new URL(this.baseUrl + path);
    for (const [k, v] of Object.entries(query ?? {})) {
      if (v === void 0 || v === null || v === "") continue;
      url.searchParams.set(k, typeof v === "boolean" ? v ? "1" : "0" : String(v));
    }
    return url.toString();
  }
  retryDelay(attempt, retryAfter) {
    if (retryAfter) {
      const secs = Number(retryAfter);
      if (Number.isFinite(secs) && secs >= 0) return Math.min(secs * 1e3, this.maxRetryDelayMs);
      const date = Date.parse(retryAfter);
      if (Number.isFinite(date)) return Math.min(Math.max(0, date - Date.now()), this.maxRetryDelayMs);
    }
    const exp = this.retryBaseMs * 2 ** attempt;
    return Math.min(exp + Math.floor(Math.random() * (this.retryBaseMs / 2)), this.maxRetryDelayMs);
  }
  async errorFrom(res) {
    let message = "";
    try {
      const text = await res.text();
      try {
        const body = JSON.parse(text);
        if (typeof body.error === "string") message = body.error;
        else if (typeof body.message === "string") message = body.message;
      } catch {
        message = text.trim().slice(0, 300);
      }
    } catch {
      message = "";
    }
    if (!message) {
      if (res.status === 401) message = 'Token do agente recusado pelo servidor. Rode "configurar" de novo com um token v\xE1lido.';
      else message = `Erro ${res.status} do servidor.`;
    }
    const retryable = res.status === 429 || res.status >= 500;
    return new ApiError(message, res.status, retryable);
  }
  // Faz a requisição com timeout e novas tentativas (429, 5xx e falha de rede; com onlyRetry429, só o 429). `consume`
  // roda dentro do timeout.
  async send(method, path, opts, consume) {
    const url = this.buildUrl(path, opts.query);
    const maxRetries = opts.retries ?? this.retries;
    const timeout = opts.timeoutMs ?? this.timeoutMs;
    for (let attempt = 0; ; attempt++) {
      if (opts.signal?.aborted) throw new ApiError("Opera\xE7\xE3o cancelada.", 0, false);
      const ctrl = new AbortController();
      let timedOut = false;
      const timer = setTimeout(() => {
        timedOut = true;
        ctrl.abort();
      }, timeout);
      const onAbort = () => ctrl.abort();
      opts.signal?.addEventListener("abort", onAbort, { once: true });
      let retryAfter = null;
      let failure;
      try {
        const headers = {
          authorization: `Bearer ${this.token}`,
          accept: "application/json",
          "user-agent": this.userAgent,
          ...this.jobId ? { [JOB_HEADER]: String(this.jobId) } : {},
          ...this.workJobId ? { [WORK_JOB_HEADER]: String(this.workJobId) } : {}
        };
        let body;
        if (opts.json !== void 0) {
          headers["content-type"] = "application/json";
          body = JSON.stringify(opts.json);
        } else if (opts.form) {
          body = await opts.form();
        }
        const res = await this.fetchFn(url, { method, headers, body, signal: ctrl.signal });
        if (res.ok) return await consume(res);
        failure = await this.errorFrom(res);
        retryAfter = res.headers.get("retry-after");
      } catch (err) {
        if (err instanceof ApiError) failure = err;
        else if (opts.signal?.aborted) throw new ApiError("Opera\xE7\xE3o cancelada.", 0, false);
        else if (timedOut) failure = new ApiError(`Tempo esgotado ao falar com o servidor (${Math.round(timeout / 1e3)} s).`, 0, true);
        else failure = new ApiError(`Sem conex\xE3o com o servidor: ${errText(err)}`, 0, true);
      } finally {
        clearTimeout(timer);
        opts.signal?.removeEventListener("abort", onAbort);
      }
      if (!failure.retryable || attempt >= maxRetries || opts.onlyRetry429 && failure.status !== 429) throw failure;
      await this.sleep(this.retryDelay(attempt, retryAfter));
    }
  }
  async requestJson(method, path, opts = {}) {
    return this.send(method, path, opts, async (res) => {
      if (res.status === 204) return null;
      const text = await res.text();
      if (!text) return null;
      try {
        return JSON.parse(text);
      } catch {
        throw new ApiError("O servidor devolveu uma resposta que n\xE3o \xE9 JSON.", res.status, false);
      }
    });
  }
  // Conta do Claude testada nesta máquina: o servidor grava neste agente e nos da mesma máquina (todos os projetos).
  claudeAccount(body) {
    return this.requestJson("POST", "/api/agent/conta-claude", { json: body, retries: 1 });
  }
  // Começar, projeto do zero: a pasta e o repositório criados (ou o motivo de não dar).
  repoCreated(body) {
    return this.requestJson("POST", "/api/agent/repo-criado", { json: body, retries: 1, timeoutMs: 3e4 });
  }
  // Começar: a leitura dos projetos desta máquina, com o id do pedido.
  machineScanReport(body) {
    return this.requestJson("POST", "/api/agent/projetos-da-maquina", { json: body, retries: 1, timeoutMs: 3e4 });
  }
  heartbeat(body, opts = {}) {
    return this.requestJson("POST", "/api/agent/heartbeat", {
      json: body,
      ...opts
    });
  }
  // Preparo da máquina: o que este executor conferiu, instalou ou não conseguiu instalar antes do trabalho.
  machinePrep(report) {
    return this.requestJson("POST", "/api/agent/preparo", { json: report, retries: 1 });
  }
  // registrar_dependencia: programa, ferramenta ou plugin que o trabalho precisou nesta máquina.
  registerDependency(body) {
    return this.requestJson("POST", "/api/agent/preparo/dependencias", { json: body });
  }
  // Resultado da conferência de merge pedida no batimento (ou o motivo de não ter dado).
  mergeCheckResult(id, body) {
    return this.requestJson("POST", `/api/agent/merge-checks/${id}`, { json: body });
  }
  // A branch do card mandada para o GitHub (fila de subida), ou o motivo de não ter dado.
  branchShareResult(id, body) {
    return this.requestJson("POST", `/api/agent/branch-shares/${id}`, { json: body });
  }
  // Os valores lidos para o Trazer os valores (vão direto para o cofre; a resposta não traz nada de volta).
  keyFetchResult(id, body) {
    return this.requestJson("POST", `/api/agent/chaves-valores/${id}`, { json: body });
  }
  // Pedido de usar este Claude em outro projeto: a entrada foi gravada e o executor ligado (ou o motivo de não ter dado).
  linkResult(id, body) {
    return this.requestJson("POST", `/api/agent/vinculos/${id}`, { json: body });
  }
  // running: os trabalhos que este executor está rodando agora (o servidor dá por perdidos os outros que ele pegou).
  async nextJob(waitSec = 25, signal, types = SUPPORTED_JOB_TYPES, caps = JOB_REQUEST_CAPS, running = []) {
    const res = await this.requestJson("GET", "/api/agent/jobs/next", {
      query: { wait: waitSec, types: types.join(","), caps: caps.join(","), running: running.join(",") },
      timeoutMs: (waitSec + 20) * 1e3,
      signal
    });
    return res?.job ?? null;
  }
  // published: só na publicação, quantos deploys o informar_publicacao já registrou (servidor antigo não manda).
  // stopped: o Claude parou a publicação de propósito, com o parar_publicacao (servidor antigo não manda).
  getJob(id, opts = {}) {
    return this.requestJson("GET", `/api/agent/jobs/${id}`, opts);
  }
  // Versão publicada do plugin (o aviso ao abrir o Claude Code consulta aqui). Servidor antigo: 404.
  pluginInfo(opts = {}) {
    return this.requestJson("GET", "/api/agent/plugin", opts);
  }
  // limit: limite de uso do plano do Claude que acabou (servidor antigo ignora o campo).
  finishJob(id, body) {
    return this.requestJson("POST", `/api/agent/jobs/${id}/finish`, { json: body });
  }
  async listTasks(filter = {}) {
    const res = await this.requestJson("GET", "/api/agent/tasks", {
      query: { status: filter.status ?? void 0, mine: filter.mine ? 1 : void 0 }
    });
    return res?.tasks ?? [];
  }
  getTask(number) {
    return this.requestJson("GET", `/api/agent/tasks/${number}`);
  }
  // Baixa o anexo para `dir` e devolve o caminho. O nome vem do Content-Disposition. `source` (X-Attachment-Source):
  // 'externo', 'widget' ou 'email' é anexo de fora do projeto (RS-S7); null se o servidor não mandou o cabeçalho.
  async downloadAttachment(id, dir) {
    return this.send("GET", `/api/agent/attachments/${id}/file`, { timeoutMs: 5 * 6e4 }, async (res) => {
      const fileName = fileNameFromDisposition(res.headers.get("content-disposition"), `anexo-${id}`);
      const path = join(dir, `${id}-${fileName}`);
      if (!res.body) throw new ApiError("O servidor n\xE3o devolveu o arquivo.", res.status, false);
      let size = 0;
      const limiter = new Transform({
        transform(chunk, _enc, cb) {
          size += chunk.length;
          if (size > MAX_DOWNLOAD_BYTES) cb(new ApiError("Anexo grande demais para baixar.", 0, false));
          else cb(null, chunk);
        }
      });
      try {
        await pipeline(Readable.fromWeb(res.body), limiter, createWriteStream(path, { mode: 384 }));
      } catch (err) {
        await rm(path, { force: true });
        throw err;
      }
      return { path, fileName, size, contentType: res.headers.get("content-type"), source: res.headers.get("x-attachment-source") };
    });
  }
  // Card da conversa do modo Claude ("Ajuste pedido pelo Telegram"): só vale dentro da conversa.
  createCard(body) {
    return this.requestJson("POST", "/api/agent/tasks", { json: body });
  }
  // Card novo pedido pelo dev no terminal (criar_chamado): como o Novo chamado do painel, no nome do dono do token.
  createTask(body) {
    return this.requestJson("POST", "/api/agent/cards", { json: body });
  }
  // Anexos soltos num card (anexar_arquivo): multipart com os arquivos em `files` (caminho local ou conteúdo já lido).
  attach(taskNumber, files) {
    const form = async () => {
      const fd = new FormData();
      for (const file of files) {
        if (typeof file === "string") {
          statSync(file);
          fd.append("files", await openAsBlob(file, { type: guessMime(file) }), basename(file));
        } else {
          const bytes = new Uint8Array(file.data.byteLength);
          bytes.set(file.data);
          fd.append("files", new Blob([bytes], { type: file.type ?? guessMime(file.name) }), basename(file.name));
        }
      }
      return fd;
    };
    return this.requestJson("POST", `/api/agent/tasks/${taskNumber}/attachments`, {
      form,
      timeoutMs: 5 * 6e4
    });
  }
  startExecution(taskNumber, promptExtra) {
    return this.requestJson("POST", `/api/agent/tasks/${taskNumber}/executions`, {
      json: { mode: "ia", ...promptExtra ? { promptExtra } : {} }
    });
  }
  // Terminal do Claude (conversa pelo painel). As três rotas valem só para o trabalho da conversa em andamento (jobId)
  // e o servidor confere de novo o acesso de quem pediu. Nenhuma tem chave de idempotência: repetir sozinho depois de
  // falha de rede, tempo esgotado ou 5xx abriria card em dobro, então só o 429 tenta de novo (onlyRetry429).
  // Lista de cards proposta neste turno (propor_cards): a pessoa confirma numa mensagem seguinte.
  panelProposal(jobId, cards) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/proposta`, { json: { cards }, onlyRetry429: true });
  }
  // Card aberto pela conversa do painel (criar_chamado): sai no nome de quem pediu, não no do dono do token.
  // attachedFiles e skippedFiles: quantos dos arquivos pedidos (fileIds) o servidor copiou para o card e quantos pulou.
  panelCreateTask(jobId, body) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/cards`, {
      json: body,
      onlyRetry429: true
    });
  }
  // Trabalho num card começado pela conversa do painel (iniciar_execucao): execução pedida por quem está na conversa.
  panelStartExecution(jobId, number, promptExtra) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/executions`, {
      json: { number, ...promptExtra ? { promptExtra } : {} },
      onlyRetry429: true
    });
  }
  // Orquestrador (só no turno dele): a leitura da esteira e as mudanças no quadro. Mudança sem chave de idempotência: só
  // o 429 tenta de novo (o resto pode ter aplicado; o Claude confere com ver_esteira ou ver_chamado).
  orchPipeline(jobId, query = {}) {
    return this.requestJson("GET", `/api/agent/jobs/${jobId}/esteira`, { query });
  }
  orchChange(jobId, change) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/quadro/mudar`, { json: change, onlyRetry429: true });
  }
  orchPropose(jobId, changes) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/quadro/proposta`, { json: { changes }, onlyRetry429: true });
  }
  orchApply(jobId, proposal, items) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/quadro/aplicar`, {
      json: { proposal, ...items?.length ? { items } : {} },
      onlyRetry429: true
    });
  }
  progress(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/progress`, { json: body });
  }
  // O terminal segue aberto (MCP no terminal do dono, a cada 5 min para cada execução que a sessão abriu ou usou): o
  // servidor segura a execução do terminal e devolve o status dela; fora de rodando, o MCP para de mandar. Sem novas
  // tentativas e com prazo curto: o próximo sinal já é a nova tentativa. Servidor de antes da rota: 404.
  terminalAlive(executionId) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/terminal`, { retries: 0, timeoutMs: 15e3 });
  }
  // Presença do terminal (ganchos do plugin no Claude Code do dono): a sessão abriu, trabalha, espera a pessoa ou fechou.
  // tracked false: a pasta não é de um repositório do projeto. Sem novas tentativas: o próximo gancho já tenta de novo.
  terminalPresence(body, timeoutMs = 4e3) {
    return this.requestJson("POST", "/api/agent/terminal/presence", { json: body, retries: 0, timeoutMs });
  }
  terminalMirror(body, timeoutMs = 4e3) {
    return this.requestJson("POST", "/api/agent/terminal/espelho", { json: body, retries: 0, timeoutMs });
  }
  // Trabalho pelo terminal: a mudança de arquivo e o commit abrem o card; testes, push e o fim da vez fazem ele andar.
  // opened: o card foi aberto (ou ligado à sessão) agora. Servidor de antes da rota: 404 (o gancho sai calado).
  terminalWork(body, timeoutMs = 6e3) {
    return this.requestJson("POST", "/api/agent/terminal/trabalho", { json: body, retries: 0, timeoutMs });
  }
  // Situação da gravação do vídeo de evidência (o card mostra ao vivo): gravando, enviando ou o erro da tentativa.
  videoProgress(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/video`, { json: body, retries: 0 });
  }
  // multipart: campos do contrato e, se houver, o arquivo em `file` (caminho local ou conteúdo já lido).
  evidence(executionId, fields, file) {
    return this.sendEvidence(`/api/agent/executions/${executionId}/evidence`, fields, file);
  }
  // Teste do card pelo botão Testar (trabalho testar): a evidência vai para o teste, não para o trabalho do card.
  testEvidence(testId, fields, file) {
    return this.sendEvidence(`/api/agent/testes/${testId}/evidence`, fields, file);
  }
  // O andamento do teste (vira a mensagem do teste no histórico do card; a etapa do card não muda).
  testProgress(testId, body) {
    return this.requestJson("POST", `/api/agent/testes/${testId}/progress`, { json: body });
  }
  sendEvidence(path, fields, file) {
    const form = async () => {
      const fd = new FormData();
      fd.append("type", fields.type);
      fd.append("title", fields.title);
      if (fields.content) fd.append("content", fields.content);
      if (fields.url) fd.append("url", fields.url);
      if (fields.passed !== void 0 && fields.passed !== null) fd.append("passed", fields.passed ? "1" : "0");
      if (fields.total !== void 0 && fields.total !== null) fd.append("total", String(fields.total));
      if (fields.failures !== void 0 && fields.failures !== null) fd.append("failures", String(fields.failures));
      if (typeof file === "string" && file) {
        statSync(file);
        const blob = await openAsBlob(file, { type: guessMime(file) });
        fd.append("file", blob, basename(file));
      } else if (file && typeof file === "object") {
        const bytes = new Uint8Array(file.data.byteLength);
        bytes.set(file.data);
        fd.append("file", new Blob([bytes], { type: file.type ?? guessMime(file.name) }), basename(file.name));
      }
      return fd;
    };
    return this.requestJson("POST", path, {
      form,
      timeoutMs: 2 * 6e4
    });
  }
  comment(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/comment`, { json: body });
  }
  // Análise do pedido com a base de conhecimento (analisar_chamado): o servidor ajusta a descrição, grava no histórico
  // o que mudou, o motivo e as notas usadas e, no conflito, comenta no card e para o trabalho (goAhead: o automático
  // liberado deixou seguir; servidor antigo: ausente).
  analysis(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/analysis`, { json: body });
  }
  // files (Portão): a lista completa de arquivos da branch, quando o git desta máquina diz; o servidor de antes ignora.
  localDone(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/local-done`, { json: body });
  }
  // parar_publicacao: a publicação não segue (sem push), com o motivo e os cards de que a mudança depende.
  publishStopped(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/publish-stopped`, { json: body });
  }
  // parar_trabalho: o trabalho do card termina sem concluir_local, com o motivo (nada a mudar ou precisa da equipe).
  workStopped(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/work-stopped`, { json: body });
  }
  // pedir_acao: o trabalho do terminal espera uma ação da pessoa (Esperando você no card), sem parar.
  workWaiting(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/waiting-user`, { json: body });
  }
  // Produção por Pull Request (servidor com publishPrs): a resposta traz o PR aberto (pr) e deployment vem null.
  published(executionId, body) {
    return this.requestJson(
      "POST",
      `/api/agent/executions/${executionId}/published`,
      { json: body }
    );
  }
  // Pedido de confirmação de um comando fora da lista liberada: o servidor guarda no card e devolve a situação.
  requestApproval(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/approvals`, { json: body, retries: 1 });
  }
  stats(executionId, body) {
    return this.requestJson("POST", `/api/agent/executions/${executionId}/stats`, { json: body });
  }
  // Envia em lotes de até 50 linhas, cada texto com até 2000 caracteres. Os tokens e a sessão vão no último lote
  // (sem linhas, vão sozinhos).
  async sendLog(executionId, lines, extras = {}) {
    await this.postLog(`/api/agent/executions/${executionId}/log`, lines, extras);
  }
  // Linha do tempo da conversa do modo Claude (vira o "Pensando..." no Telegram).
  async jobLog(jobId, lines, extras = {}) {
    await this.postLog(`/api/agent/jobs/${jobId}/log`, lines, extras);
  }
  async postLog(path, lines, extras) {
    const clean = lines.map((l) => ({ at: l.at, kind: l.kind, text: l.text.slice(0, MAX_LOG_TEXT), ...l.diff ? { diff: l.diff } : {} }));
    const extra = { ...extras.usage ? { usage: extras.usage } : {}, ...extras.session ? { session: extras.session } : {} };
    if (!clean.length) {
      if (Object.keys(extra).length) await this.requestJson("POST", path, { json: { lines: [], ...extra } });
      return;
    }
    for (let i = 0; i < clean.length; i += MAX_LOG_LINES) {
      const last = i + MAX_LOG_LINES >= clean.length;
      await this.requestJson("POST", path, { json: { lines: clean.slice(i, i + MAX_LOG_LINES), ...last ? extra : {} } });
    }
  }
  // Sugestões de cards (ata ou arquivo) geradas por este Claude: o servidor valida e grava.
  // repos: onde o código foi conferido ("dono/repo", com a branch e o commit em refs).
  suggestionsResult(jobId, output, repos = [], refs = []) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/sugestoes`, { json: { output, repos, refs } });
  }
  // Atualização do cofre feita: o resumo e as notas criadas ou mudadas (caminhos dentro do cofre).
  vaultResult(jobId, body) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/cofre`, { json: body });
  }
  // Cofre enviado ao Benflow: o pacote (JSON com as notas, em gzip) vai para a homologação do projeto.
  uploadVault(pkg) {
    const form = async () => {
      const fd = new FormData();
      fd.append("cofre", new Blob([new Uint8Array(pkg)], { type: "application/gzip" }), "cofre.json.gz");
      return fd;
    };
    return this.requestJson("POST", "/api/agent/cofre/envio", { form, timeoutMs: 5 * 6e4, retries: 1 });
  }
  // Leitura do cofre (modo estruturar): baixa a homologação (pacote em gzip).
  downloadVaultHomolog(jobId) {
    return this.send("GET", `/api/agent/jobs/${jobId}/cofre-homologacao`, { timeoutMs: 5 * 6e4 }, async (res) => Buffer.from(await res.arrayBuffer()));
  }
  vaultReadProgress(jobId, stage, progress) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/cofre-progresso`, { json: { stage, progress }, retries: 0 });
  }
  // Fim da leitura: o resumo (vai antes do arquivo, o servidor lê os campos que chegam antes dele) e as notas novas.
  vaultReadResult(jobId, summary, pkg) {
    const form = async () => {
      const fd = new FormData();
      fd.append("summary", summary);
      if (pkg) fd.append("cofre", new Blob([new Uint8Array(pkg)], { type: "application/gzip" }), "estrutura.json.gz");
      return fd;
    };
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/cofre-estrutura`, { form, timeoutMs: 5 * 6e4 });
  }
  // Atualizar a base no cofre enviado: baixa a produção (pacote em gzip).
  downloadVaultProduction(jobId) {
    return this.send("GET", `/api/agent/jobs/${jobId}/cofre-producao`, { timeoutMs: 5 * 6e4 }, async (res) => Buffer.from(await res.arrayBuffer()));
  }
  vaultOrganizeProgress(jobId, stage, progress) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/cofre-organizar/progresso`, { json: { stage, progress }, retries: 0 });
  }
  // Fim do Atualizar a base: o resultado (vai antes do arquivo) e, no cofre enviado, as notas mudadas.
  vaultOrganizeResult(jobId, result, pkg) {
    const form = async () => {
      const fd = new FormData();
      fd.append("resultado", JSON.stringify(result));
      if (pkg) fd.append("cofre", new Blob([new Uint8Array(pkg)], { type: "application/gzip" }), "base.json.gz");
      return fd;
    };
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/cofre-organizar`, { form, timeoutMs: 5 * 6e4 });
  }
  // Fim da atualização depois da produção no cofre enviado: o resumo, as notas e o hash do conteúdo recebido de cada uma
  // (vai antes do arquivo) e o pacote das notas mudadas.
  vaultEnvioResult(jobId, result, pkg) {
    const form = async () => {
      const fd = new FormData();
      fd.append("resultado", JSON.stringify(result));
      if (pkg) fd.append("cofre", new Blob([new Uint8Array(pkg)], { type: "application/gzip" }), "notas.json.gz");
      return fd;
    };
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/cofre-notas`, { form, timeoutMs: 5 * 6e4 });
  }
  // Resposta final da conversa do modo Claude (o servidor entrega no Telegram).
  jobResult(jobId, body) {
    return this.requestJson("POST", `/api/agent/jobs/${jobId}/result`, { json: body });
  }
  // Arquivo mandado na conversa (foto, documento) ou referência de um trabalho sugestoes para `dir`, com o nome dado.
  // Devolve o caminho. signal: o trabalho foi cancelado no meio do download.
  async downloadJobFile(jobId, fileId, dir, name, opts = {}) {
    return this.send("GET", `/api/agent/jobs/${jobId}/files/${fileId}`, { timeoutMs: 5 * 6e4, signal: opts.signal }, async (res) => {
      const safe = fileNameFromDisposition(`attachment; filename="${name.replace(/"/g, "")}"`, `arquivo-${fileId}`);
      const path = join(dir, `${fileId}-${safe}`);
      if (!res.body) throw new ApiError("O servidor n\xE3o devolveu o arquivo.", res.status, false);
      let size = 0;
      const limiter = new Transform({
        transform(chunk, _enc, cb) {
          size += chunk.length;
          if (size > MAX_DOWNLOAD_BYTES) cb(new ApiError("Arquivo grande demais para baixar.", 0, false));
          else cb(null, chunk);
        }
      });
      try {
        await pipeline(Readable.fromWeb(res.body), limiter, createWriteStream(path, { mode: 384 }));
      } catch (err) {
        await rm(path, { force: true });
        throw err;
      }
      return path;
    });
  }
  async searchKnowledge(q, limit = 8) {
    const res = await this.requestJson("GET", "/api/agent/knowledge/search", { query: { q, limit } });
    return res?.results ?? [];
  }
  readNote(path) {
    return this.requestJson("GET", "/api/agent/knowledge/note", { query: { path } });
  }
  environments() {
    return this.requestJson("GET", "/api/agent/environments");
  }
  // configurar_ambientes (prompt de conexões): repositórios e, de cada ambiente, branch, endereço e URL de saúde. Só o
  // administrador do projeto; o servidor nunca tira um repositório por aqui.
  saveEnvironments(body) {
    return this.requestJson("PUT", "/api/agent/environments", { json: body });
  }
  // Usuário de teste do ambiente (Ambientes), para o passo entrar: só dentro do trabalho que abre aquele ambiente
  // (X-Benflow-Trabalho). null: o projeto não tem usuário de teste nele. Nunca vai para o Claude.
  async environmentLogin(key) {
    const res = await this.requestJson("GET", `/api/agent/environments/${key}/login`);
    const login = res?.login;
    return login && typeof login.user === "string" && typeof login.password === "string" && login.user && login.password ? login : null;
  }
};

// connector/config.ts
import { chmodSync, existsSync, mkdirSync, readFileSync, realpathSync, renameSync, statSync as statSync2, writeFileSync } from "node:fs";
import os from "node:os";
import { dirname, join as join2, resolve } from "node:path";

// connector/codex.ts
var CODEX_SANDBOXES = ["workspace-write", "danger-full-access"];
function defaultCodexSettings() {
  return { bin: "codex", model: null, sandbox: "workspace-write", network: true };
}

// connector/legado.ts
var LEGACY_CONFIG_DIRS = [".bora", ".chamados"];
var LEGACY_ENV_PREFIXES = ["BORA_", "CHAMADOS_"];
function legacyEnvKeys(name) {
  return LEGACY_ENV_PREFIXES.map((prefix) => `${prefix}${name}`);
}
function migrateAllowedTool(rule) {
  const t = rule.trim();
  const plugin = /^mcp__plugin_bora_bora(__.*)?$/.exec(t);
  if (plugin) return `mcp__plugin_benflow_benflow${plugin[1] ?? ""}`;
  const server = /^mcp__(?:chamados|bora)(__.*)?$/.exec(t);
  if (server) return `mcp__benflow${server[1] ?? ""}`;
  return rule;
}
function migrateAllowedTools(rules) {
  const out = [];
  for (const rule of rules) {
    const next = migrateAllowedTool(rule);
    if (!out.includes(next)) out.push(next);
  }
  return out;
}

// connector/config.ts
function isAiProvider(v) {
  return v === "claude" || v === "codex";
}
function entryProvider(entry) {
  return entry.provider === "codex" ? "codex" : "claude";
}
function providerName(provider) {
  return provider === "codex" ? "Codex" : "Claude";
}
var MACHINE_ID_RE = /^[A-Za-z0-9_-]{8,80}$/;
var DEFAULT_ALLOWED_TOOLS = [
  "Read",
  "Edit",
  "Write",
  "Glob",
  "Grep",
  "Bash(git status*)",
  "Bash(git diff*)",
  "Bash(git log*)",
  "Bash(git show*)",
  "Bash(git add*)",
  "Bash(git commit*)",
  "Bash(git checkout*)",
  "Bash(git switch*)",
  "Bash(git restore*)",
  // fetch e pull só do origin (com URL no comando viram canal de saída de dados).
  "Bash(git fetch)",
  "Bash(git fetch origin*)",
  "Bash(git fetch --prune origin*)",
  "Bash(git pull)",
  "Bash(git pull origin*)",
  "Bash(git pull --ff-only)",
  "Bash(git pull --ff-only origin*)",
  "Bash(git pull --rebase origin*)",
  "Bash(git merge*)",
  "Bash(git rebase*)",
  "Bash(git cherry-pick*)",
  "Bash(git rev-parse*)",
  "Bash(git branch*)",
  "Bash(git stash*)",
  "Bash(git remote -v)",
  "Bash(npm test*)",
  "Bash(npm run test*)",
  "Bash(npm run lint*)",
  "Bash(npm run typecheck*)",
  "Bash(npm run build*)",
  "Bash(npm run dev*)",
  "Bash(npx tsc*)",
  "Bash(npx vitest*)",
  "Bash(npx eslint*)",
  "mcp__benflow"
];
var LEGACY_DEFAULT_ALLOWED_TOOLS = [
  "Read",
  "Edit",
  "Write",
  "Glob",
  "Grep",
  "Bash(git *)",
  "Bash(npm *)",
  "Bash(npx *)",
  "Bash(node *)",
  "mcp__chamados"
];
var FETCH_RULES = /* @__PURE__ */ new Set([
  "Bash(git fetch)",
  "Bash(git fetch origin*)",
  "Bash(git fetch --prune origin*)",
  "Bash(git pull)",
  "Bash(git pull origin*)",
  "Bash(git pull --ff-only)",
  "Bash(git pull --ff-only origin*)",
  "Bash(git pull --rebase origin*)"
]);
var PHASE_B_DEFAULT_ALLOWED_TOOLS = DEFAULT_ALLOWED_TOOLS.flatMap(
  (t) => t === "Bash(git fetch)" ? ["Bash(git fetch*)"] : t === "Bash(git pull)" ? ["Bash(git pull*)"] : FETCH_RULES.has(t) ? [] : [t]
);
var PHASE_A_DEFAULT_ALLOWED_TOOLS = PHASE_B_DEFAULT_ALLOWED_TOOLS.filter((t) => t !== "Bash(npm run dev*)");
function sameList(a, b) {
  return a.length === b.length && [...a].sort().join("\n") === [...b].sort().join("\n");
}
function defaultClaudeSettings() {
  return { bin: "claude", model: null, permissionMode: "acceptEdits", allowedTools: [...DEFAULT_ALLOWED_TOOLS], maxTurns: 200 };
}
function expandHome(p) {
  if (p === "~") return os.homedir();
  if (p.startsWith("~/") || p.startsWith("~\\")) return join2(os.homedir(), p.slice(2));
  return p;
}
function defaultConfigDir() {
  return join2(os.homedir(), ".benflow");
}
function legacyConfigDirs() {
  return LEGACY_CONFIG_DIRS.map((dir) => join2(os.homedir(), dir));
}
function envValue(env, ...keys) {
  for (const k of keys) {
    const v = env[k]?.trim();
    if (v && !/^\$\{[^}]*\}$/.test(v)) return v;
  }
  return null;
}
function benflowEnv(env, name, ...extra) {
  return envValue(env, `BENFLOW_${name}`, ...legacyEnvKeys(name), ...extra);
}
function configPath(env = process.env) {
  const custom = benflowEnv(env, "CONFIG");
  if (custom) return resolve(expandHome(custom));
  return join2(defaultConfigDir(), "config.json");
}
function readableConfigPath(path = configPath()) {
  if (existsSync(path)) return path;
  if (resolve(path) !== resolve(join2(defaultConfigDir(), "config.json"))) return path;
  for (const dir of legacyConfigDirs()) {
    const legacy = join2(dir, "config.json");
    if (existsSync(legacy)) return legacy;
  }
  return path;
}
function orgFromEnv(env = process.env) {
  return benflowEnv(env, "ORG");
}
function isLoopbackHost(hostname) {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  return host === "localhost" || host.endsWith(".localhost") || host === "::1" || /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host);
}
function normalizeUrl(raw) {
  const value = raw.trim();
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`URL inv\xE1lida: ${value}`);
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new Error(`A URL precisa come\xE7ar com http:// ou https://: ${value}`);
  if (parsed.protocol === "http:" && !isLoopbackHost(parsed.hostname)) {
    throw new Error(`Use https:// para um servidor fora desta m\xE1quina (com http:// o token do agente trafega sem criptografia): ${parsed.origin}`);
  }
  return `${parsed.origin}${parsed.pathname.replace(/\/+$/, "")}`;
}
function asString(v) {
  return typeof v === "string" && v.trim() ? v.trim() : null;
}
function normalizeClaude(raw) {
  const base = defaultClaudeSettings();
  if (!raw || typeof raw !== "object") return base;
  const r = raw;
  const tools = Array.isArray(r.allowedTools) ? migrateAllowedTools(r.allowedTools.filter((t) => typeof t === "string" && t.trim() !== "")) : null;
  const maxTurns = typeof r.maxTurns === "number" && Number.isInteger(r.maxTurns) && r.maxTurns > 0 ? r.maxTurns : base.maxTurns;
  return {
    bin: asString(r.bin) ?? base.bin,
    model: asString(r.model),
    permissionMode: asString(r.permissionMode) ?? base.permissionMode,
    allowedTools: tools && !sameList(tools, migrateAllowedTools(LEGACY_DEFAULT_ALLOWED_TOOLS)) && !sameList(tools, PHASE_A_DEFAULT_ALLOWED_TOOLS) && !sameList(tools, PHASE_B_DEFAULT_ALLOWED_TOOLS) ? tools : base.allowedTools,
    maxTurns
  };
}
function normalizeEntry(raw, index) {
  if (!raw || typeof raw !== "object") throw new Error(`Servidor ${index + 1} do config est\xE1 inv\xE1lido.`);
  const r = raw;
  const url = asString(r.url);
  const token = asString(r.token);
  if (!url || !token) throw new Error(`Servidor ${index + 1} do config precisa de url e token.`);
  const repos = {};
  if (r.repos && typeof r.repos === "object" && !Array.isArray(r.repos)) {
    for (const [name, path] of Object.entries(r.repos)) {
      if (typeof path === "string" && path.trim()) repos[name] = path.trim();
    }
  }
  return {
    url: normalizeUrl(url),
    token,
    orgSlug: asString(r.orgSlug),
    repos,
    personalVault: asString(r.personalVault),
    projectVault: asString(r.projectVault),
    claude: normalizeClaude(r.claude),
    ...r.provider === "codex" ? { provider: "codex", codex: normalizeCodex(r.codex) } : {}
  };
}
function normalizeCodex(raw) {
  const base = defaultCodexSettings();
  if (!raw || typeof raw !== "object") return base;
  const r = raw;
  return {
    bin: asString(r.bin) ?? base.bin,
    model: asString(r.model),
    sandbox: CODEX_SANDBOXES.includes(String(r.sandbox)) ? r.sandbox : base.sandbox,
    network: typeof r.network === "boolean" ? r.network : base.network
  };
}
function parseConfig(text, path = "config.json") {
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`N\xE3o foi poss\xEDvel ler ${path}: JSON inv\xE1lido.`);
  }
  const servers = data && typeof data === "object" && Array.isArray(data.servers) ? data.servers : [];
  const machineId = data && typeof data === "object" ? data.machineId : void 0;
  return {
    servers: servers.map((s, i) => normalizeEntry(s, i)),
    ...typeof machineId === "string" && MACHINE_ID_RE.test(machineId) ? { machineId } : {}
  };
}
function loadConfig(requested = configPath(), warn = () => {
}) {
  const path = readableConfigPath(requested);
  if (!existsSync(path)) return { servers: [] };
  try {
    const mode = statSync2(path).mode & 511;
    if (process.platform !== "win32" && (mode & 63) !== 0) {
      warn(`Aviso: ${path} tem permiss\xE3o ${mode.toString(8)}; o certo \xE9 600 (chmod 600 "${path}").`);
    }
  } catch {
  }
  return parseConfig(readFileSync(path, "utf8"), path);
}
function pickServer(cfg, orgSlug, url, provider) {
  const slug = orgSlug?.trim();
  const rawUrl = url?.trim();
  const servers = provider ? cfg.servers.filter((s) => entryProvider(s) === provider) : [...cfg.servers.filter((s) => entryProvider(s) === "claude"), ...cfg.servers.filter((s) => entryProvider(s) !== "claude")];
  const which = provider ? ` com o ${providerName(provider)}` : "";
  if (rawUrl) {
    const wanted = normalizeUrl(rawUrl);
    const found = servers.find((s) => s.url === wanted && (!slug || s.orgSlug === slug));
    if (!found) throw new Error(`Este computador n\xE3o est\xE1 ligado a ${wanted}${slug ? ` (organiza\xE7\xE3o "${slug}")` : ""}${which}. Gere um token na Equipe IA desse painel e rode o configurar.`);
    return found;
  }
  if (slug) {
    const found = servers.find((s) => s.orgSlug === slug);
    if (!found) throw new Error(`Nenhum servidor configurado para a organiza\xE7\xE3o "${slug}"${which}.`);
    return found;
  }
  const first = servers[0];
  if (!first) throw new Error(provider === "codex" ? NOT_CONFIGURED_CODEX : NOT_CONFIGURED);
  return first;
}
function providerFromEnv(env = process.env) {
  const v = benflowEnv(env, "IA")?.toLowerCase();
  return isAiProvider(v) ? v : null;
}
var NOT_CONFIGURED = "O Benflow ainda n\xE3o est\xE1 configurado nesta m\xE1quina. No Claude Code, abra /plugin, escolha o plugin benflow e preencha o endere\xE7o e o token do agente (Equipe IA > Ligar meu Claude), ou rode: node benflow.mjs configurar --url <endere\xE7o>.";
var NOT_CONFIGURED_CODEX = "O Benflow ainda n\xE3o est\xE1 configurado para o Codex nesta m\xE1quina. Gere o token em Equipe IA > Agentes > IAs, escolha o Codex > Ligar meu Codex e rode: node benflow.mjs configurar --url <endere\xE7o> (o Benflow guarda o token no ~/.benflow/config.json).";
function resolveServer(cfg, env = process.env, cwd = process.cwd()) {
  const rawUrl = benflowEnv(env, "URL", "CLAUDE_PLUGIN_OPTION_URL");
  const token = benflowEnv(env, "TOKEN", "CLAUDE_PLUGIN_OPTION_TOKEN");
  const org = orgFromEnv(env);
  const provider = providerFromEnv(env);
  const ordered = provider ? { ...cfg, servers: [...cfg.servers.filter((s) => entryProvider(s) === provider), ...cfg.servers.filter((s) => entryProvider(s) !== provider)] } : cfg;
  if (rawUrl && token) {
    const url = normalizeUrl(rawUrl);
    const same = ordered.servers.filter((s) => s.url === url);
    const base = same.find((s) => s.token === token) ?? (org ? same.find((s) => s.orgSlug === org) : void 0) ?? (same.length === 1 ? same[0] : void 0);
    return {
      url,
      token,
      orgSlug: base?.orgSlug ?? org ?? null,
      repos: base?.repos ?? {},
      personalVault: base?.personalVault ?? null,
      projectVault: base?.projectVault ?? null,
      claude: base?.claude ?? defaultClaudeSettings(),
      ...base && entryProvider(base) === "codex" ? { provider: "codex", codex: base.codex ?? defaultCodexSettings() } : {}
    };
  }
  if (!ordered.servers.length) return null;
  if (rawUrl) return pickServer(ordered, org, rawUrl, provider && ordered.servers.some((s) => s.url === normalizeUrl(rawUrl) && entryProvider(s) === provider) ? provider : null);
  if (!org) {
    const here = serverForFolder(ordered.servers, provider === "codex" && env.PWD ? env.PWD : cwd);
    if (here) return here;
  }
  if (provider && !org) return ordered.servers[0];
  return pickServer(ordered, org, null, provider && ordered.servers.some((s) => entryProvider(s) === provider && (!org || s.orgSlug === org)) ? provider : null);
}
function realOrSelf(p) {
  try {
    return realpathSync.native(p);
  } catch {
    return p;
  }
}
function serverForFolder(servers, folder) {
  const norm2 = (p) => {
    const r = realOrSelf(resolve(p)).replace(/[\\/]+$/, "");
    return process.platform === "win32" || process.platform === "darwin" ? r.toLowerCase() : r;
  };
  const here = norm2(folder);
  for (const s of servers) {
    for (const path of Object.values(s.repos ?? {})) {
      if (!path) continue;
      const repo = norm2(path);
      if (here === repo || here.startsWith(`${repo}/`) || here.startsWith(`${repo}\\`)) return s;
    }
  }
  return null;
}

// connector/local.ts
import { chmodSync as chmodSync2, existsSync as existsSync2, mkdirSync as mkdirSync2, readFileSync as readFileSync2, readlinkSync, realpathSync as realpathSync2, renameSync as renameSync2, rmSync, statSync as statSync3, writeFileSync as writeFileSync2 } from "node:fs";
function norm(p, platform) {
  const s = p.replace(/\\/g, "/").replace(/\/+$/, "");
  return platform === "darwin" || platform === "win32" ? s.toLowerCase() : s;
}
function realOr(p) {
  try {
    return realpathSync2.native(p);
  } catch {
    return p;
  }
}
function repoFromRemote(url) {
  const m = /[/:]([\w.-]+)\/([\w.-]+?)(?:\.git)?\/?$/.exec(url.trim());
  return m ? `${m[1]}/${m[2]}` : null;
}
function repoForPath(dir, repos, platform = process.platform) {
  const target = norm(realOr(dir), platform);
  let best = null;
  for (const [name, path] of Object.entries(repos)) {
    for (const base of /* @__PURE__ */ new Set([norm(path, platform), norm(realOr(path), platform)])) {
      if (!base) continue;
      if (target === base || target.startsWith(`${base}/`)) {
        if (!best || base.length > best.len) best = { name, len: base.length };
      }
    }
  }
  return best?.name ?? null;
}
var LOCAL_KEEP_DOWN_MS = 30 * 6e4;
var LOCAL_KEEP_DOWN_RECIPE_MS = 7 * 24 * 60 * 6e4;

// connector/version.ts
import { existsSync as existsSync3, readFileSync as readFileSync3 } from "node:fs";
import { dirname as dirname2, join as join3 } from "node:path";
import { fileURLToPath } from "node:url";
var cached = null;
function connectorVersion() {
  if (cached) return cached;
  if ("0.1.75") {
    cached = "0.1.75";
    return cached;
  }
  let dir = dirname2(fileURLToPath(import.meta.url));
  for (let i = 0; i < 5; i++) {
    const file = join3(dir, "package.json");
    if (existsSync3(file)) {
      try {
        const pkg = JSON.parse(readFileSync3(file, "utf8"));
        if (pkg.version) {
          cached = pkg.version;
          return cached;
        }
      } catch {
      }
    }
    const parent = dirname2(dir);
    if (parent === dir) break;
    dir = parent;
  }
  cached = "0.0.0";
  return cached;
}

// connector/presence.ts
var PRESENCE_THROTTLE_MS = 6e4;
var STATE_KEEP_MS = 3 * 24 * 60 * 6e4;
var OFF_VALUES = /* @__PURE__ */ new Set(["0", "false", "nao", "n\xE3o", "off", "desligada", "desligado"]);
var BENFLOW_TOOL_RE = /^mcp__(?:plugin_[\w-]+_)?benflow__/;
function parseInput(text) {
  const empty = { sessionId: null, cwd: null, event: null, toolName: null };
  if (!text) return empty;
  try {
    const d = JSON.parse(text);
    const s = (v) => typeof v === "string" && v ? v : null;
    return { sessionId: s(d.session_id), cwd: s(d.cwd), event: s(d.hook_event_name), toolName: s(d.tool_name) };
  } catch {
    return empty;
  }
}
function presenceStateOf(event, toolName = null) {
  switch (event) {
    case "SessionStart":
      return { state: "esperando", benflowTool: false };
    case "UserPromptSubmit":
      return { state: "trabalhando", benflowTool: false };
    case "PostToolUse":
      return { state: "trabalhando", benflowTool: !!toolName && BENFLOW_TOOL_RE.test(toolName) };
    case "Stop":
      return { state: "esperando", benflowTool: false };
    case "SessionEnd":
      return { state: "fechada", benflowTool: false };
    default:
      return null;
  }
}
function presenceSessionKey(sessionId) {
  return createHash("sha256").update(`benflow-presenca:${sessionId}`, "utf8").digest("base64url").slice(0, 40);
}
function machineNameOf(hostname) {
  const name = hostname.trim().split(".")[0]?.trim() ?? "";
  return name ? name.slice(0, 80) : null;
}
function sameRepo(a, b) {
  return a.toLowerCase() === b.toLowerCase();
}
function stateDir(env) {
  return join4(dirname3(configPath(env)), "presenca");
}
function readState(file) {
  try {
    const d = JSON.parse(readFileSync4(file, "utf8"));
    return {
      state: d.state === "trabalhando" || d.state === "esperando" || d.state === "fechada" ? d.state : null,
      sentAt: typeof d.sentAt === "number" ? d.sentAt : 0,
      off: d.off === true,
      targets: Array.isArray(d.targets) ? d.targets.filter((t) => t && typeof t.url === "string").map((t) => ({ url: t.url, org: typeof t.org === "string" ? t.org : null })) : []
    };
  } catch {
    return null;
  }
}
function writeState(file, state) {
  try {
    mkdirSync3(dirname3(file), { recursive: true, mode: 448 });
    writeFileSync3(file, JSON.stringify(state), { mode: 384 });
  } catch {
  }
}
function dropState(file) {
  try {
    rmSync2(file, { force: true });
  } catch {
  }
}
function pruneStates(dir, now) {
  try {
    for (const name of readdirSync(dir)) {
      if (!name.endsWith(".json")) continue;
      const file = join4(dir, name);
      if (now - statSync4(file).mtimeMs > STATE_KEEP_MS) rmSync2(file, { force: true });
    }
  } catch {
  }
}
async function gitInfo(exec2, cwd) {
  const [remote, head] = await Promise.all([
    exec2("git", ["remote", "get-url", "origin"], { cwd, timeoutMs: 3e3 }).catch(() => null),
    exec2("git", ["rev-parse", "--abbrev-ref", "HEAD", "--show-toplevel"], { cwd, timeoutMs: 3e3 }).catch(() => null)
  ]);
  const repo = remote && remote.code === 0 ? repoFromRemote(remote.stdout) : null;
  const lines = head && head.code === 0 ? head.stdout.split(/\r?\n/).map((l) => l.trim()).filter(Boolean) : [];
  const branch = lines[0] && lines[0] !== "HEAD" ? lines[0] : null;
  const folder = lines[1] ? basename2(lines[1]) || null : null;
  return { repo, branch, folder };
}
function addEntry(list, e) {
  if (!list.some((o) => o.url === e.url && o.token === e.token)) list.push(e);
}
function presenceTargets(cfg, resolved, repo, benflowTool) {
  const out = [];
  if (repo) {
    for (const s of cfg.servers) if (Object.keys(s.repos).some((r) => sameRepo(r, repo))) addEntry(out, s);
  }
  if (resolved && (benflowTool || repo && !out.length)) addEntry(out, resolved);
  return out;
}
function storedTargets(cfg, resolved, stored) {
  const all = [...resolved ? [resolved] : [], ...cfg.servers];
  const out = [];
  for (const t of stored) {
    const e = all.find((s) => s.url === t.url && (s.orgSlug ?? null) === t.org);
    if (e) addEntry(out, e);
  }
  return out;
}
async function presenceHook(deps) {
  const env = deps.env ?? process.env;
  if (benflowEnv(env, "EXECUTOR") === "1" || benflowEnv(env, "EXECUTION_ID")) return "executor";
  const off = benflowEnv(env, "PRESENCA");
  if (off && OFF_VALUES.has(off.trim().toLowerCase())) return "desligada";
  const input = parseInput(deps.stdinText);
  const mapped = presenceStateOf(input.event, input.toolName);
  if (!input.sessionId || !mapped) return "ignorado";
  const { state, benflowTool } = mapped;
  const now = (deps.now ?? Date.now)();
  const key = presenceSessionKey(input.sessionId);
  const dir = stateDir(env);
  const file = join4(dir, `${key}.json`);
  const local = readState(file);
  if (input.event === "SessionStart") pruneStates(dir, now);
  if (local?.off && !benflowTool) {
    if (state === "fechada") dropState(file);
    return "fora";
  }
  if (local && !local.off && local.state === state && state !== "fechada" && now - local.sentAt < PRESENCE_THROTTLE_MS) return "segurado";
  if (!local && state === "fechada") return "ignorado";
  let cfg;
  let resolved = null;
  try {
    cfg = loadConfig(readableConfigPath(configPath(env)));
    resolved = resolveServer(cfg, env);
  } catch {
    return "sem-config";
  }
  if (!cfg.servers.length && !resolved) return "sem-config";
  const cwd = input.cwd || deps.cwd || process.cwd();
  const git = await gitInfo(deps.exec, cwd);
  let repo = git.repo;
  if (!repo) for (const s of [...resolved ? [resolved] : [], ...cfg.servers]) repo ??= repoForPath(cwd, s.repos);
  const targets = local && !local.off && local.targets.length ? storedTargets(cfg, resolved, local.targets) : presenceTargets(cfg, resolved, repo, benflowTool);
  if (benflowTool && resolved) addEntry(targets, resolved);
  if (!targets.length) {
    if (state === "fechada") dropState(file);
    else writeState(file, { state: null, sentAt: now, off: true, targets: [] });
    return "fora";
  }
  const body = {
    session: key,
    state,
    repo,
    branch: git.branch,
    folder: git.folder,
    machineId: typeof cfg.machineId === "string" && MACHINE_ID_RE.test(cfg.machineId) ? cfg.machineId : null,
    machineName: machineNameOf((deps.hostname ?? os2.hostname)()),
    platform: deps.platform ?? process.platform,
    version: deps.version ?? connectorVersion(),
    ...benflowTool ? { benflowTool: true } : {}
  };
  const results = await Promise.allSettled(
    targets.map(
      (e) => new AgentClient({ url: e.url, token: e.token, fetch: deps.fetch, retries: 0, timeoutMs: deps.timeoutMs ?? 4e3, userAgent: `benflow-conector/${body.version} presenca` }).terminalPresence(
        body,
        deps.timeoutMs ?? 4e3
      )
    )
  );
  const kept = targets.filter((_, i) => {
    const r = results[i];
    return r.status === "rejected" || r.value?.tracked === true;
  });
  if (state === "fechada") {
    dropState(file);
    return "enviado";
  }
  if (!kept.length) {
    writeState(file, { state: null, sentAt: now, off: true, targets: [] });
    return "fora";
  }
  writeState(file, { state, sentAt: now, off: false, targets: kept.map((e) => ({ url: e.url, org: e.orgSlug ?? null })) });
  return "enviado";
}

// connector/terminalMirror.ts
import { closeSync, mkdirSync as mkdirSync4, openSync, readFileSync as readFileSync5, readSync, rmSync as rmSync3, statSync as statSync5, writeFileSync as writeFileSync4 } from "node:fs";
import os3 from "node:os";
import { basename as basename3, dirname as dirname4, join as join5 } from "node:path";
var MIRROR_TEXT_MAX = 8e3;
var MIRROR_ANSWER_MAX = 6e4;
var MIRROR_LINES_MAX = 60;
var LINE_MAX = 1e3;
var TRANSCRIPT_TAIL_BYTES = 4 * 1024 * 1024;
var MIRROR_TIMEOUT_MS = 3500;
function parseInput2(text) {
  const empty = { sessionId: null, cwd: null, event: null, prompt: null, transcriptPath: null, lastMessage: null };
  if (!text) return empty;
  try {
    const d = JSON.parse(text);
    const s = (v) => typeof v === "string" && v ? v : null;
    return { sessionId: s(d.session_id), cwd: s(d.cwd), event: s(d.hook_event_name), prompt: s(d.prompt), transcriptPath: s(d.transcript_path), lastMessage: s(d.last_assistant_message) };
  } catch {
    return empty;
  }
}
function cut(text, max) {
  return text.length > max ? `${text.slice(0, max - 3)}...` : text;
}
function oneLine(text, max) {
  return cut(text.replace(/\s+/g, " ").trim(), max);
}
function mirrorFile(env, key) {
  return join5(stateDir(env), `${key}.espelho.json`);
}
function readMirror(file) {
  try {
    const d = JSON.parse(readFileSync5(file, "utf8"));
    if (typeof d.seq !== "number" || !Number.isSafeInteger(d.seq) || d.seq < 1) return null;
    const targets = Array.isArray(d.targets) ? d.targets.filter((t) => t && typeof t.url === "string").map((t) => ({ url: t.url, org: typeof t.org === "string" ? t.org : null })) : [];
    return { seq: d.seq, prompt: typeof d.prompt === "string" ? d.prompt : "", targets };
  } catch {
    return null;
  }
}
function writeMirror(file, state) {
  try {
    mkdirSync4(dirname4(file), { recursive: true, mode: 448 });
    writeFileSync4(file, JSON.stringify(state), { mode: 384 });
  } catch {
  }
}
function toolStep(name, input) {
  const s = (v) => typeof v === "string" && v.trim() ? v.trim() : null;
  const file = (v) => s(v) ? basename3(s(v)) : null;
  const benflow = /^mcp__(?:plugin_[\w-]+_)?benflow__(.+)$/.exec(name);
  if (benflow) return `Benflow: ${benflow[1]}`;
  const mcp = /^mcp__[\w-]+__(.+)$/.exec(name);
  if (mcp) return mcp[1];
  switch (name) {
    case "Bash":
      return s(input.description) ? `Comando: ${s(input.description)}` : "Rodou um comando no terminal";
    case "Read":
      return file(input.file_path) ? `Leu ${file(input.file_path)}` : "Leu um arquivo";
    case "Edit":
    case "MultiEdit":
      return file(input.file_path) ? `Editou ${file(input.file_path)}` : "Editou um arquivo";
    case "Write":
      return file(input.file_path) ? `Escreveu ${file(input.file_path)}` : "Escreveu um arquivo";
    case "NotebookEdit":
      return file(input.notebook_path) ? `Editou ${file(input.notebook_path)}` : "Editou um notebook";
    case "Grep":
      return "Procurou no c\xF3digo";
    case "Glob":
      return "Procurou arquivos";
    case "WebSearch":
      return "Pesquisou na web";
    case "WebFetch": {
      try {
        return `Abriu ${new URL(s(input.url) ?? "").host}`;
      } catch {
        return "Abriu uma p\xE1gina";
      }
    }
    case "Task":
    case "Agent":
      return s(input.description) ? `Subagente: ${s(input.description)}` : "Chamou um subagente";
    case "TodoWrite":
    case "TaskCreate":
    case "TaskUpdate":
      return "Atualizou a lista de tarefas";
    default:
      return name;
  }
}
function promptOf(e) {
  if (e.type !== "user" || e.isMeta === true || e.isSidechain === true) return null;
  const c = e.message?.content;
  if (typeof c === "string") return c.trim() && !/^<(local-)?command-/.test(c.trim()) ? c.trim() : null;
  if (!Array.isArray(c) || c.some((b) => b?.type === "tool_result")) return null;
  const text = c.filter((b) => b?.type === "text").map((b) => String(b.text ?? "")).join("\n").trim();
  return text || null;
}
function readTranscriptTurn(path, maxBytes = TRANSCRIPT_TAIL_BYTES) {
  const none = { prompt: null, promptAt: null, answer: null, lines: [] };
  if (!path || !path.endsWith(".jsonl")) return none;
  let text;
  try {
    const size = statSync5(path).size;
    const start = Math.max(0, size - maxBytes);
    const fd = openSync(path, "r");
    try {
      const buf = Buffer.alloc(size - start);
      readSync(fd, buf, 0, buf.length, start);
      text = buf.toString("utf8");
    } finally {
      closeSync(fd);
    }
    if (start > 0) text = text.slice(text.indexOf("\n") + 1);
  } catch {
    return none;
  }
  const entries = [];
  for (const raw of text.split("\n")) {
    if (!raw.trim()) continue;
    try {
      entries.push(JSON.parse(raw));
    } catch {
    }
  }
  let from = -1;
  for (let i = entries.length - 1; i >= 0; i--) {
    if (promptOf(entries[i])) {
      from = i;
      break;
    }
  }
  const prompt = from >= 0 ? promptOf(entries[from]) : null;
  const promptAt = from >= 0 && typeof entries[from].timestamp === "string" ? entries[from].timestamp : null;
  const lines = [];
  let answer = null;
  for (const e of entries.slice(from + 1)) {
    if (e.type !== "assistant" || e.isSidechain === true || !Array.isArray(e.message?.content)) continue;
    const at = typeof e.timestamp === "string" ? e.timestamp : null;
    for (const b of e.message.content) {
      if (b?.type === "text" && typeof b.text === "string" && b.text.trim()) {
        lines.push({ at, kind: "texto", text: oneLine(b.text, LINE_MAX) });
        answer = b.text.trim();
      } else if (b?.type === "tool_use" && typeof b.name === "string") {
        lines.push({ at, kind: "ferramenta", text: oneLine(toolStep(b.name, b.input ?? {}), LINE_MAX) });
      }
    }
  }
  if (answer && lines.at(-1)?.kind === "texto") lines.pop();
  return { prompt, promptAt, answer, lines: lines.slice(-MIRROR_LINES_MAX) };
}
async function mirrorHook(deps) {
  const env = deps.env ?? process.env;
  if (benflowEnv(env, "EXECUTOR") === "1" || benflowEnv(env, "EXECUTION_ID")) return "executor";
  for (const name of ["PRESENCA", "ESPELHO"]) {
    const v = benflowEnv(env, name);
    if (v && OFF_VALUES.has(v.trim().toLowerCase())) return "desligado";
  }
  const input = parseInput2(deps.stdinText);
  if (!input.sessionId || input.event !== "UserPromptSubmit" && input.event !== "Stop" && input.event !== "SessionEnd") return "ignorado";
  const key = presenceSessionKey(input.sessionId);
  const presence = readState(join5(stateDir(env), `${key}.json`));
  const file = mirrorFile(env, key);
  if (input.event === "SessionEnd" && !presence) {
  } else if (!presence || presence.off || !presence.targets.length) {
    return "fora";
  }
  let cfg;
  let resolved = null;
  try {
    cfg = loadConfig(readableConfigPath(configPath(env)));
    resolved = resolveServer(cfg, env);
  } catch {
    return "sem-config";
  }
  const mine = readMirror(file);
  const targets = storedTargets(cfg, resolved, presence?.targets.length ? presence.targets : mine?.targets ?? []);
  if (!targets.length) return "fora";
  const cwd = input.cwd || deps.cwd || process.cwd();
  const git = await gitInfo(deps.exec, cwd);
  let repo = git.repo;
  if (!repo) for (const s of [...resolved ? [resolved] : [], ...cfg.servers]) repo ??= repoForPath(cwd, s.repos);
  if (!repo && input.event !== "SessionEnd") return "fora";
  const now = (deps.now ?? Date.now)();
  const place = { repo, branch: git.branch, folder: git.folder, machineName: machineNameOf((deps.hostname ?? os3.hostname)()) };
  let body;
  if (input.event === "UserPromptSubmit") {
    const prompt = cut((input.prompt ?? "").trim(), MIRROR_TEXT_MAX);
    if (!prompt) return "ignorado";
    const seq = Math.max(now, (mine?.seq ?? 0) + 1);
    writeMirror(file, { seq, prompt, targets: targets.map((e) => ({ url: e.url, org: e.orgSlug ?? null })) });
    body = { session: key, event: "pedido", seq, text: prompt, ...place };
  } else if (input.event === "Stop") {
    const turn = readTranscriptTurn(input.transcriptPath);
    const seq = mine?.seq ?? (turn.promptAt && Number.isFinite(Date.parse(turn.promptAt)) ? Date.parse(turn.promptAt) : now);
    const answer = (input.lastMessage ?? turn.answer ?? "").trim();
    body = { session: key, event: "resposta", seq, text: cut(mine?.prompt || turn.prompt || "", MIRROR_TEXT_MAX), answer: cut(answer, MIRROR_ANSWER_MAX), lines: turn.lines, ...place };
  } else {
    if (!mine) return "ignorado";
    body = { session: key, event: "fechada", seq: mine.seq, ...place };
  }
  const version = deps.version ?? connectorVersion();
  await Promise.allSettled(
    targets.map(
      (e) => new AgentClient({ url: e.url, token: e.token, fetch: deps.fetch, retries: 0, timeoutMs: deps.timeoutMs ?? MIRROR_TIMEOUT_MS, userAgent: `benflow-conector/${version} espelho` }).terminalMirror(
        body,
        deps.timeoutMs ?? MIRROR_TIMEOUT_MS
      )
    )
  );
  if (input.event === "SessionEnd") {
    try {
      rmSync3(file, { force: true });
    } catch {
    }
  }
  return "enviado";
}

// connector/terminalWork.ts
import { mkdirSync as mkdirSync5, readFileSync as readFileSync6, writeFileSync as writeFileSync5 } from "node:fs";
import os4 from "node:os";
import { basename as basename4, dirname as dirname5, isAbsolute, join as join6, resolve as resolve2 } from "node:path";
var FILE_TOOLS = /* @__PURE__ */ new Set(["Edit", "Write", "MultiEdit", "NotebookEdit"]);
var START_TOOL_RE = /^mcp__(?:plugin_[\w-]+_)?benflow__iniciar_execucao$/;
var COMMIT_RE = /\bgit\s+(?:-C\s+\S+\s+)?commit\b/;
var PUSH_RE = /\bgit\s+(?:-C\s+\S+\s+)?push\b/;
var TEST_RE = /\b(?:npm|pnpm|yarn|bun)\s+(?:run\s+)?(?:test|t)(?:\b|:)|\bnpx\s+(?:vitest|jest|playwright)\b|\bvitest\b|\bjest\b|\bpytest\b|\bgo\s+test\b|\bcargo\s+test\b|\bphpunit\b|\bmvn\s+test\b|\bgradlew?\s+test\b|\brspec\b|\bplaywright\s+test\b/;
var COMMIT_FRESH_MS = 10 * 6e4;
var PROMPT_MAX = 8e3;
var WORK_TIMEOUT_MS = 6e3;
function parseInput3(text) {
  const empty = { sessionId: null, cwd: null, event: null, toolName: null, toolInput: {}, toolResponse: null, prompt: null, transcriptPath: null };
  if (!text) return empty;
  try {
    const d = JSON.parse(text);
    const s = (v) => typeof v === "string" && v ? v : null;
    const input = d.tool_input && typeof d.tool_input === "object" ? d.tool_input : {};
    return { sessionId: s(d.session_id), cwd: s(d.cwd), event: s(d.hook_event_name), toolName: s(d.tool_name), toolInput: input, toolResponse: d.tool_response ?? null, prompt: s(d.prompt), transcriptPath: s(d.transcript_path) };
  } catch {
    return empty;
  }
}
function readJson(file) {
  try {
    return JSON.parse(readFileSync6(file, "utf8"));
  } catch {
    return null;
  }
}
function writeJson(file, data) {
  try {
    mkdirSync5(dirname5(file), { recursive: true, mode: 448 });
    writeFileSync5(file, JSON.stringify(data), { mode: 384 });
  } catch {
  }
}
function readWork(file) {
  const d = readJson(file);
  return {
    sent: d?.sent && typeof d.sent === "object" ? d.sent : {},
    card: d?.card && typeof d.card === "object" && typeof d.card.number === "number" ? d.card : null,
    base: typeof d?.base === "string" ? d.base : null,
    repoDir: typeof d?.repoDir === "string" ? d.repoDir : null,
    off: Array.isArray(d?.off) ? d.off.filter((r) => typeof r === "string") : []
  };
}
function startedExecutionId(response) {
  const text = typeof response === "string" ? response : JSON.stringify(response ?? "");
  const m = /Execu\S{1,8}o #(\d+) iniciada/i.exec(text);
  const n = m ? Number(m[1]) : NaN;
  return Number.isSafeInteger(n) && n > 0 ? n : null;
}
function workEventOf(toolName, toolInput) {
  if (!toolName) return null;
  if (FILE_TOOLS.has(toolName)) {
    const file = typeof toolInput.file_path === "string" ? toolInput.file_path : typeof toolInput.notebook_path === "string" ? toolInput.notebook_path : null;
    return file ? { event: "mudanca", file, command: null } : null;
  }
  if (START_TOOL_RE.test(toolName)) return { event: "ligar", file: null, command: null };
  if (toolName !== "Bash") return null;
  const command = typeof toolInput.command === "string" ? toolInput.command : "";
  if (COMMIT_RE.test(command)) return { event: "commit", file: null, command };
  if (PUSH_RE.test(command)) return { event: "push", file: null, command };
  if (TEST_RE.test(command)) return { event: "testes", file: null, command };
  return null;
}
async function gitAt(exec2, dir) {
  const head = await exec2("git", ["rev-parse", "--show-toplevel", "--abbrev-ref", "HEAD"], { cwd: dir, timeoutMs: 3e3 }).catch(() => null);
  if (!head || head.code !== 0) return null;
  const lines = head.stdout.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const top = lines[0];
  if (!top) return null;
  const remote = await exec2("git", ["remote", "get-url", "origin"], { cwd: top, timeoutMs: 3e3 }).catch(() => null);
  return { top, repo: remote && remote.code === 0 ? repoFromRemote(remote.stdout) : null, branch: lines[1] && lines[1] !== "HEAD" ? lines[1] : null };
}
function inside(file, top) {
  const f = resolve2(file);
  const t = resolve2(top);
  return f === t || f.startsWith(`${t}/`) || f.startsWith(`${t}\\`);
}
function targetsFor(cfg, resolved, repo, dir) {
  let name = repo;
  if (!name) for (const s of [...resolved ? [resolved] : [], ...cfg.servers]) name ??= repoForPath(dir, s.repos);
  return presenceTargets(cfg, resolved, name, false);
}
function contextText(card, base) {
  const link = base ? ` (${base.replace(/\/+$/, "")}${card.path})` : "";
  return [
    `Benflow: o trabalho desta sess\xE3o est\xE1 no card #${card.number}${link}, aberto sozinho pelo terminal com o t\xEDtulo provis\xF3rio "${card.title}" e o pedido da pessoa s\xF3 no Pedido original.`,
    `Antes de seguir com o c\xF3digo, analise o pedido com a base de conhecimento (buscar_conhecimento e ler_nota) e o c\xF3digo, e chame analisar_chamado com numero ${card.number}, resultado "ajustado", titulo (curto, o que muda para quem usa), descricao (o que vai ser feito, onde no sistema e como conferir), mudancas, motivo e impacto (resumo, areas, perfis e regras afetados).`,
    `Use a etiqueta [${card.tag}] no come\xE7o de cada commit. O card anda sozinho com as mudan\xE7as, os testes, os commits, o push e o fim da vez.`
  ].join(" ");
}
function promptContextText(card, base) {
  const link = base ? ` (${base.replace(/\/+$/, "")}${card.path})` : "";
  return [
    `Benflow: esta sess\xE3o est\xE1 no card #${card.number} "${card.title}"${link}.`,
    `Se este pedido for outra tarefa (outra tela, outro problema ou outra mudan\xE7a, e n\xE3o um ajuste, uma corre\xE7\xE3o ou a continua\xE7\xE3o do #${card.number}), abra um card novo antes de mexer no c\xF3digo: iniciar_execucao sem numero, com titulo e descricao escritos por voc\xEA a partir do pedido. O #${card.number} fica como est\xE1, e cada tarefa nova pedida nesta mensagem ganha o seu card.`,
    `Se for do #${card.number}, siga nele. Pergunta, explica\xE7\xE3o ou conversa n\xE3o abrem card.`,
    `Se voc\xEA precisar que a pessoa fa\xE7a algo para seguir (entrar numa conta, rodar um comando que pede senha, liberar algo no navegador), chame pedir_acao no card antes de pedir aqui: o card mostra em vermelho o que falta.`
  ].join(" ");
}
function workPromptHook(deps) {
  const env = deps.env ?? process.env;
  if (benflowEnv(env, "EXECUTOR") === "1" || benflowEnv(env, "EXECUTION_ID")) return null;
  const input = parseInput3(deps.stdinText);
  if (!input.sessionId || input.event !== "UserPromptSubmit") return null;
  const key = presenceSessionKey(input.sessionId);
  const file = join6(stateDir(env), `${key}.pedido.json`);
  const old = readJson(file);
  writeJson(file, { turn: (typeof old?.turn === "number" ? old.turn : 0) + 1, prompt: (input.prompt ?? "").trim().slice(0, PROMPT_MAX) });
  for (const name of ["PRESENCA", "CARD_TERMINAL"]) {
    const v = benflowEnv(env, name);
    if (v && OFF_VALUES.has(v.trim().toLowerCase())) return null;
  }
  const work = readWork(join6(stateDir(env), `${key}.trabalho.json`));
  if (!work.card) return null;
  return JSON.stringify({ hookSpecificOutput: { hookEventName: "UserPromptSubmit", additionalContext: promptContextText(work.card, work.base) } });
}
async function workHook(deps) {
  const result = (outcome, extra = {}) => ({ outcome, event: null, card: null, opened: false, output: null, ...extra });
  const env = deps.env ?? process.env;
  if (benflowEnv(env, "EXECUTOR") === "1" || benflowEnv(env, "EXECUTION_ID")) return result("executor");
  for (const name of ["PRESENCA", "CARD_TERMINAL"]) {
    const v = benflowEnv(env, name);
    if (v && OFF_VALUES.has(v.trim().toLowerCase())) return result("desligado");
  }
  const input = parseInput3(deps.stdinText);
  if (!input.sessionId) return result("ignorado");
  const key = presenceSessionKey(input.sessionId);
  const workFile = join6(stateDir(env), `${key}.trabalho.json`);
  const state = readWork(workFile);
  const cwd = input.cwd || deps.cwd || process.cwd();
  let found;
  if (input.event === "PostToolUse") found = workEventOf(input.toolName, input.toolInput);
  else if (input.event === "Stop") found = state.card ? { event: "fim", file: null, command: null } : null;
  else return result("ignorado");
  if (!found) return result("nada");
  const event = found.event;
  if ((event === "testes" || event === "push") && !state.card) return result("nada", { event });
  const prompt = readJson(join6(stateDir(env), `${key}.pedido.json`));
  const turn = typeof prompt?.turn === "number" ? prompt.turn : 0;
  if ((event === "mudanca" || event === "testes") && state.card && state.sent[event] === turn) return result("nada", { event, card: state.card });
  const dir = found.file ? dirname5(isAbsolute(found.file) ? found.file : join6(cwd, found.file)) : event === "fim" && state.repoDir ? state.repoDir : cwd;
  const git = await gitAt(deps.exec, dir);
  if (!git) return result("fora", { event });
  if (found.file && !inside(isAbsolute(found.file) ? found.file : join6(cwd, found.file), git.top)) return result("fora", { event });
  if (git.repo && state.off.includes(git.repo.toLowerCase())) return result("fora", { event });
  let cfg;
  let resolved = null;
  try {
    cfg = loadConfig(readableConfigPath(configPath(env)));
    resolved = resolveServer(cfg, env);
  } catch {
    return result("sem-config", { event });
  }
  const targets = targetsFor(cfg, resolved, git.repo, git.top);
  if (!targets.length) return result("fora", { event });
  const body = {
    session: key,
    event,
    repo: git.repo,
    branch: git.branch,
    folder: basename4(git.top) || null,
    machineName: machineNameOf((deps.hostname ?? os4.hostname)())
  };
  if (event === "mudanca" || event === "commit") {
    body.prompt = (prompt?.prompt || readTranscriptTurn(input.transcriptPath).prompt || "").slice(0, PROMPT_MAX) || null;
  }
  if (event === "commit") {
    const log = await deps.exec("git", ["log", "-1", "--format=%H%x1f%ct%x1f%s"], { cwd: git.top, timeoutMs: 3e3 }).catch(() => null);
    const [sha, at, subject] = log && log.code === 0 ? log.stdout.trim().split("") : [];
    const now = (deps.now ?? Date.now)();
    if (!sha || !/^[0-9a-f]{40}$/.test(sha) || now - Number(at) * 1e3 > COMMIT_FRESH_MS) return result("nada", { event });
    body.commit = { sha, message: (subject ?? "").slice(0, 500) || null };
  }
  if (event === "ligar") {
    const id = startedExecutionId(input.toolResponse);
    if (!id) return result("nada", { event });
    body.executionId = id;
  }
  if (event === "fim") {
    const status = await deps.exec("git", ["status", "--porcelain"], { cwd: git.top, timeoutMs: 3e3 }).catch(() => null);
    body.clean = !!status && status.code === 0 && status.stdout.trim() === "";
  }
  const version = deps.version ?? connectorVersion();
  const results = await Promise.allSettled(
    targets.map(
      (e) => new AgentClient({ url: e.url, token: e.token, fetch: deps.fetch, retries: 0, timeoutMs: deps.timeoutMs ?? WORK_TIMEOUT_MS, userAgent: `benflow-conector/${version} trabalho` }).terminalWork(
        body,
        deps.timeoutMs ?? WORK_TIMEOUT_MS
      )
    )
  );
  const answers = results.map((r, i) => ({ entry: targets[i], value: r.status === "fulfilled" ? r.value : null }));
  const tracked = answers.filter((a) => a.value?.tracked);
  if (git.repo && answers.every((a) => a.value && !a.value.tracked)) {
    writeJson(workFile, { ...state, off: [...state.off, git.repo.toLowerCase()] });
    return result("fora", { event });
  }
  const main = tracked.find((a) => a.value?.card) ?? tracked[0];
  const card = main?.value?.card ?? state.card;
  const opened = tracked.some((a) => a.value?.opened);
  const sent = { ...state.sent };
  if (main?.value?.card && (event === "mudanca" || event === "testes")) sent[event] = turn;
  if (opened) sent.mudanca = turn;
  writeJson(workFile, { sent, card: main?.value?.card ?? state.card, base: main?.value?.card ? main.entry.url : state.base, repoDir: git.top, off: state.off });
  const output = opened && main?.value?.card && input.event === "PostToolUse" && event !== "ligar" ? JSON.stringify({ hookSpecificOutput: { hookEventName: "PostToolUse", additionalContext: contextText(main.value.card, main.entry.url) } }) : null;
  return result(tracked.length ? "enviado" : "fora", { event, card, opened, output });
}

// connector/presenca.ts
var exec = (cmd, args, opts = {}) => new Promise((done) => {
  execFile(cmd, args, { cwd: opts.cwd, timeout: opts.timeoutMs ?? 3e3, windowsHide: true, encoding: "utf8" }, (err, stdout, stderr) => {
    const code = err ? typeof err.code === "number" ? err.code : 1 : 0;
    done({ code, stdout: String(stdout ?? ""), stderr: String(stderr ?? "") });
  });
});
function readStdin(ms) {
  if (process.stdin.isTTY) return Promise.resolve("");
  return new Promise((done) => {
    const chunks = [];
    const finish = () => {
      clearTimeout(timer);
      process.stdin.removeAllListeners("data");
      process.stdin.pause();
      done(Buffer.concat(chunks).toString("utf8"));
    };
    const timer = setTimeout(finish, ms);
    process.stdin.on("data", (c) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(String(c))));
    process.stdin.once("end", finish);
    process.stdin.once("error", finish);
  });
}
function eventOf(text) {
  try {
    const e = JSON.parse(text).hook_event_name;
    return typeof e === "string" ? e : null;
  } catch {
    return null;
  }
}
var guard = setTimeout(() => process.exit(0), 9500);
var workOnly = process.argv[2] === "trabalho";
var promptOnly = process.argv[2] === "pedido";
readStdin(1500).then(async (stdinText) => {
  if (workOnly) {
    const res = await workHook({ exec, stdinText }).catch(() => null);
    if (res?.output) process.stdout.write(res.output);
    return;
  }
  if (promptOnly) {
    let out = null;
    try {
      out = workPromptHook({ exec, stdinText });
    } catch {
    }
    if (out) process.stdout.write(out);
    return;
  }
  const event = eventOf(stdinText);
  if (event === "SessionEnd") {
    await Promise.allSettled([presenceHook({ exec, stdinText }), mirrorHook({ exec, stdinText })]);
    return;
  }
  await presenceHook({ exec, stdinText }).catch(() => null);
  await mirrorHook({ exec, stdinText }).catch(() => null);
  if (event === "Stop") await workHook({ exec, stdinText }).catch(() => null);
}).catch(() => null).finally(() => {
  clearTimeout(guard);
  process.exit(0);
});
