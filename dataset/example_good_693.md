```elixir
defmodule Integrations.SlackNotifier do
  @moduledoc """
  Sends structured notifications to Slack channels via the Incoming
  Webhooks API. Supports plain text messages and rich Block Kit payloads.
  All requests are retried up to three times with exponential backoff on
  transient failures. Rate-limit responses are respected via the
  `Retry-After` header. The module holds no process state.
  """

  require Logger

  @type channel :: String.t()
  @type block :: map()
  @type send_result :: :ok | {:error, :rate_limited | :webhook_error | :delivery_failed}

  @max_retries 3
  @initial_backoff_ms 500

  @doc "Sends a plain text `message` to the Slack webhook configured for `channel`."
  @spec notify(channel(), String.t()) :: send_result()
  def notify(channel, message) when is_binary(channel) and is_binary(message) do
    payload = %{text: message, channel: channel}
    post_with_retry(payload, 1)
  end

  @doc "Sends a rich Block Kit message to the configured Slack webhook."
  @spec notify_blocks(channel(), String.t(), [block()]) :: send_result()
  def notify_blocks(channel, fallback_text, blocks)
      when is_binary(channel) and is_binary(fallback_text) and is_list(blocks) do
    payload = %{text: fallback_text, channel: channel, blocks: blocks}
    post_with_retry(payload, 1)
  end

  @doc "Builds a Slack section block containing `text`."
  @spec section_block(String.t()) :: block()
  def section_block(text) when is_binary(text) do
    %{type: "section", text: %{type: "mrkdwn", text: text}}
  end

  @doc "Builds a Slack header block with bold `title` text."
  @spec header_block(String.t()) :: block()
  def header_block(title) when is_binary(title) do
    %{type: "header", text: %{type: "plain_text", text: title, emoji: true}}
  end

  @doc "Builds a divider block for visual separation."
  @spec divider_block() :: block()
  def divider_block, do: %{type: "divider"}

  defp post_with_retry(_payload, attempt) when attempt > @max_retries do
    {:error, :delivery_failed}
  end

  defp post_with_retry(payload, attempt) do
    url = webhook_url()
    body = Jason.encode!(payload)
    headers = [{"Content-Type", "application/json"}]

    case HTTPoison.post(url, body, headers, recv_timeout: 10_000) do
      {:ok, %{status_code: 200}} ->
        :ok

      {:ok, %{status_code: 429, headers: resp_headers}} ->
        retry_after = parse_retry_after(resp_headers)
        Logger.warning("[SlackNotifier] Rate limited, backing off #{retry_after}ms")
        Process.sleep(retry_after)
        post_with_retry(payload, attempt + 1)

      {:ok, %{status_code: code}} when code in 500..503 ->
        delay = backoff(attempt)
        Logger.warning("[SlackNotifier] Slack error #{code}, retry #{attempt} in #{delay}ms")
        Process.sleep(delay)
        post_with_retry(payload, attempt + 1)

      {:ok, _} ->
        {:error, :webhook_error}

      {:error, _} ->
        delay = backoff(attempt)
        Process.sleep(delay)
        post_with_retry(payload, attempt + 1)
    end
  end

  defp parse_retry_after(headers) do
    case List.keyfind(headers, "Retry-After", 0) do
      {_, value} ->
        case Integer.parse(value) do
          {secs, _} -> secs * 1_000
          _ -> @initial_backoff_ms
        end

      nil ->
        @initial_backoff_ms
    end
  end

  defp backoff(attempt), do: @initial_backoff_ms * trunc(:math.pow(2, attempt - 1))

  defp webhook_url, do: Application.fetch_env!(:my_app, :slack_webhook_url)
end
```
