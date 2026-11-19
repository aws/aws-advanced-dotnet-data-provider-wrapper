# Configuring the Wrapper with AI Tools

If you use an AI coding assistant (Kiro, Cursor, Claude Code, GitHub Copilot, ChatGPT, Amazon Bedrock, etc.), you can use the wrapper's configuration assistant skill to help build, review, or troubleshoot your wrapper configuration.

The skill is a single self-contained markdown file: [`DOTNET-WRAPPER-CONFIGURATION-ASSISTANT.md`](../DOTNET-WRAPPER-CONFIGURATION-ASSISTANT.md). It covers every plugin, parameter, default value, dialect, failover mode, NuGet package, and compatibility constraint of release 3.0.0, together with recipes for ADO.NET with Npgsql, MySqlConnector, and MySql.Data, Entity Framework Core, and NHibernate, so the AI can answer configuration questions without access to the wrapper source.

The file follows the [Agent Skills](https://agentskills.io) format. It is large: about 156,000 tokens on Claude Opus 5.5 (other models count tokens differently), and the AI receives it again with every message. Tools that cache the system prompt handle it far more cheaply than tools that resend it in full each time.

## Quick start

1. Download or copy [`DOTNET-WRAPPER-CONFIGURATION-ASSISTANT.md`](../DOTNET-WRAPPER-CONFIGURATION-ASSISTANT.md).
2. Add it to your AI tool:
   - **Tools that support Agent Skills** (Kiro, Cursor, Claude Code, GitHub Copilot in VS Code, and others): create a folder named `dotnet-wrapper-configuration-assistant` in one of the tool's skills directories, and save the file inside it as `SKILL.md`. The folder name must match the `name` field at the top of the file; otherwise the tool does not load the skill.

     | Tool | Project skills directory | User skills directory |
     |---|---|---|
     | Kiro | `.kiro/skills/` | `~/.kiro/skills/` |
     | Cursor | `.cursor/skills/`, `.agents/skills/`, or `.claude/skills/` | `~/.cursor/skills/`, `~/.agents/skills/`, or `~/.claude/skills/` |
     | Claude Code | `.claude/skills/` | `~/.claude/skills/` |
     | GitHub Copilot in VS Code | `.github/skills/`, `.agents/skills/`, or `.claude/skills/` | `~/.copilot/skills/`, `~/.agents/skills/`, or `~/.claude/skills/` |

     For example, to add the skill to a project for Claude Code, Cursor, and VS Code at once:

     ```bash
     mkdir -p .claude/skills/dotnet-wrapper-configuration-assistant
     curl -fsSL -o .claude/skills/dotnet-wrapper-configuration-assistant/SKILL.md \
       https://raw.githubusercontent.com/aws/aws-advanced-dotnet-data-provider-wrapper/main/docs/DOTNET-WRAPPER-CONFIGURATION-ASSISTANT.md
     ```

   - **Claude Projects**: upload it as a project knowledge document.
   - **ChatGPT custom GPT**: upload it as a knowledge file.
   - **Amazon Bedrock**: send the file as the system prompt of your `InvokeModel` or `Converse` request. Avoid attaching it to a Bedrock knowledge base: a knowledge base passes the model only the passages that match each question, so the skill's interview steps and rules (section 1 of the file) may be missing.
   - **Plain chat (ChatGPT, Claude, Gemini)**: paste it at the start of a new conversation, then ask your question.
3. Ask the AI for help. It will interview you when it needs more details, or give you a configuration straight away if you ask it to skip the interview.

## Example prompts

The simplest way to start:

```
Help me configure the AWS Advanced .NET Data Provider Wrapper for my application.
```

Skip the interview when you already know what you want:

```
Give me a default configuration for Aurora PostgreSQL with Entity Framework Core, Npgsql, and failover support.
```

Review an existing configuration:

```
Review this wrapper configuration and tell me what to change:

<paste your connection string, AwsWrapperConnection code, or DbContext registration here>
```

Diagnose an error:

```
My service on Aurora MySQL with MySqlConnector fails to connect with
"InvalidOperationException: Invalid usage of Host Monitoring plugin with Mysql
dialect." What's wrong?
```

The skill targets release 3.0.0 and avoids guessing: if it doesn't know something, it says so and points you at the official docs.
