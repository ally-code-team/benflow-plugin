# Plugin do Benflow para o Claude Code

Leva os chamados do Benflow para dentro do Claude Code (terminal, extensões do VS Code e JetBrains e app desktop, no Mac
e no Windows). Você conversa normalmente ("o que tem pra fazer?", "pega o chamado 12") e o Claude lista as tarefas,
mostra o card, pergunta o que fazer e vai atualizando etapa, evidências e ambiente local no painel.

O que vem no plugin:

- Servidor MCP `benflow` (arquivo único `benflow/server/benflow.mjs`, só precisa do Node 22 ou mais novo).
- Skills: `/benflow` (conduz a conversa), `/benflow:trabalhar <número>` e `/benflow:status`.
- Hook de início de sessão: ao abrir o Claude Code numa pasta de repositório da organização, mostra um resumo dos
  chamados abertos. Sem configuração ou sem rede, fica calado.

Nada vai para o GitHub sem você pedir: a publicação em homologação e produção continua pelos botões do painel.

## Antes de instalar

1. No painel do Benflow, abra **Equipe IA > Ligar meu Claude** e copie o endereço e o token do seu agente.
2. Confira o Node: `node --version` (22 ou mais novo).

## Instalar no Mac

No Claude Code:

```
/plugin marketplace add ally-code-team/benflow-plugin
/plugin install benflow@benflow-marketplace
```

Nesta ordem: sem o primeiro, o segundo responde "Marketplace benflow-marketplace not found". Depois rode `/reload-plugins`.
O Claude Code pede o endereço e o token (o token fica no Keychain). Depois abra o Claude Code na pasta do repositório
e pergunte "o que tem pra fazer?".

Para testar a partir desta pasta, sem GitHub: `/plugin marketplace add /caminho/para/benflow/plugin`.

Quem tinha o plugin antigo (`bora@bora-marketplace`): rode `/plugin uninstall bora@bora-marketplace` e
`/plugin marketplace remove bora-marketplace` (o marketplace antigo) antes de instalar o novo. O endereço e o token
precisam ser preenchidos de novo no `/plugin` (o Claude Code guarda por plugin); até lá o conector usa o
`~/.benflow/config.json`.

## Instalar no Windows

1. Instale o Node 22 (`winget install OpenJS.NodeJS.LTS`) e o Claude Code (instalador nativo ou `npm i -g
   @anthropic-ai/claude-code`).
2. No Claude Code, os mesmos comandos do Mac: `/plugin marketplace add ally-code-team/benflow-plugin` e `/plugin install benflow@benflow-marketplace`.
3. O conector acha sozinho o `claude.exe` ou o `claude.cmd` do npm e aceita pastas com espaço no nome. A configuração
   do terminal fica em `%USERPROFILE%\.benflow\config.json`.

## Executor (botão "Com IA" do painel)

Opcional: só para o painel mandar trabalho para o seu Claude. O `/benflow:status` mostra o caminho do `benflow.mjs`.

```
node "<caminho>/benflow.mjs" configurar --url <endereço> --repo owner/nome=/caminho/do/repositorio
node "<caminho>/benflow.mjs" executar
```

No Windows (PowerShell), use aspas no caminho: `node "C:\Users\Ana Maria\...\benflow.mjs" executar`.
O executor manda o batimento a cada 30 s, com os servidores de desenvolvimento que estiverem no ar (detectados sozinho
ou informados pelo Claude), e atende a "Mandar instrução" da Janela da IA retomando a mesma sessão do Claude.
Ele também:

- usa o modelo e o effort escolhidos no painel ou no Telegram (`--model` e `--effort`; sem escolha, o `model` do
  config);
- manda os tokens ao vivo e os detalhes da sessão (modelo, versão do Claude Code, turnos, ferramentas) para a Janela
  da IA enquanto o Claude trabalha;
- atende o **modo Claude** do Telegram (`/claude` no privado do bot): cada mensagem vira um `claude -p` na pasta do
  primeiro repositório do config, com o MCP do Benflow e sem push, retomando a sessão da conversa;
- atende o **Terminal do Claude** do painel (Criar cards > Terminal do Claude), a mesma conversa por mensagens dentro
  da tela, com os cards no nome de quem pediu. São três níveis, decididos pelo servidor: o **dono** do Claude roda com
  as preferências dele; um **administrador** do projeto usando o Claude liberado de outra pessoa pode pedir
  desenvolvimento, só nas pastas do projeto e sem as liberações pessoais do dono; os **outros membros** conversam,
  consultam o código do projeto e abrem cards, sem Bash, sem edição e sem começar trabalho. No turno de quem não é o
  dono, o Claude roda só com uma lista fechada de ferramentas (`--tools`: ler e buscar para os outros membros; ler,
  buscar, editar e Bash para o administrador), mais as do Benflow, e sem a memória automática do dono;
- em nenhum trabalho (card, publicação ou conversa) o Claude usa as ferramentas que alcançam outras sessões do Claude
  Code da máquina ou a conta do dono: `SendMessage`, `ListAgents`, `RemoteTrigger`, `CronCreate`, `CronDelete`,
  `CronList` e `PushNotification` ficam negadas.

Depois de atualizar o plugin, reinicie o `executar`: o executor antigo não anuncia o modo Claude (o bot pede para
reiniciar) nem a conversa pelo painel (a tela mostra "Plugin antigo").
Quem já usava o conector antigo não precisa refazer nada: os antigos `~/.bora/config.json` e
`~/.chamados/config.json` continuam sendo lidos.

## Para quem mantém

- `npm run build:connector` gera `benflow/server/benflow.mjs` a partir de `connector/` (commite o arquivo gerado: o plugin
  é instalado direto do repositório, sem etapa de build).
- Suba a `version` em `benflow/.claude-plugin/plugin.json` a cada entrega e valide com
  `claude plugin validate plugin/benflow` e `claude plugin validate plugin`.
- Para publicar, esta pasta `plugin/` vira a raiz de um repositório no GitHub (o `.claude-plugin/marketplace.json`
  precisa ficar na raiz).
