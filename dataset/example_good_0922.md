```elixir
defmodule AppWeb.Plugs.ServerSentEvents do
  @moduledoc """
  A Plug that upgrades an HTTP connection to a Server-Sent Events (SSE)
  stream, allowing the server to push typed events to browser clients over
  a persistent HTTP connection.

  Callers subscribe to a PubSub topic and forward received messages as
  SSE frames. The connection is held open until the client disconnects
  or the server-side process exits.
  """

  import Plug.Conn
  require Logger

  @behaviour Plug

  @keepalive_interval_ms 25_000
  @comment_frame ": keepalive\n\n"

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, opts) do
    topic = Keyword.fetch!(opts, :topic)
    pubsub = Keyword.get(opts, :pubsub, Platform.PubSub)
    event_mapper = Keyword.get(opts, :event_mapper, &default_mapper/1)

    conn
    |> put_resp_header("content-type", "text/event-stream")
    |> put_resp_header("cache-control", "no-cache")
    |> put_resp_header("connection", "keep-alive")
    |> put_resp_header("x-accel-buffering", "no")
    |> send_chunked(200)
    |> stream_events(topic, pubsub, event_mapper)
  end

  @doc "Encodes a map into a valid SSE frame string."
  @spec encode_event(map()) :: String.t()
  def encode_event(%{event: event, data: data} = opts) do
    id_line = if id = Map.get(opts, :id), do: "id: #{id}\n", else: ""
    retry_line = if retry = Map.get(opts, :retry_ms), do: "retry: #{retry}\n", else: ""
    data_encoded = if is_map(data) or is_list(data), do: Jason.encode!(data), else: to_string(data)

    "#{id_line}#{retry_line}event: #{event}\ndata: #{data_encoded}\n\n"
  end

  def encode_event(%{data: data}) do
    data_encoded = if is_map(data) or is_list(data), do: Jason.encode!(data), else: to_string(data)
    "data: #{data_encoded}\n\n"
  end

  defp stream_events(conn, topic, pubsub, event_mapper) do
    Phoenix.PubSub.subscribe(pubsub, topic)
    schedule_keepalive()
    loop(conn, event_mapper)
  end

  defp loop(conn, event_mapper) do
    receive do
      :keepalive ->
        case chunk(conn, @comment_frame) do
          {:ok, conn} ->
            schedule_keepalive()
            loop(conn, event_mapper)

          {:error, :closed} ->
            conn
        end

      {:sse_event, event_data} ->
        frame = encode_event(event_mapper.(event_data))

        case chunk(conn, frame) do
          {:ok, conn} -> loop(conn, event_mapper)
          {:error, :closed} -> conn
        end

      {:domain_event, payload} ->
        frame = encode_event(%{event: "message", data: payload})

        case chunk(conn, frame) do
          {:ok, conn} -> loop(conn, event_mapper)
          {:error, :closed} -> conn
        end
    after
      @keepalive_interval_ms + 1_000 ->
        conn
    end
  end

  defp schedule_keepalive do
    Process.send_after(self(), :keepalive, @keepalive_interval_ms)
  end

  defp default_mapper(data) when is_map(data), do: %{event: "message", data: data}
  defp default_mapper(data), do: %{event: "message", data: to_string(data)}
end

defmodule AppWeb.LiveFeedController do
  @moduledoc """
  Controller that streams live feed events to subscribers via SSE.
  """

  use AppWeb, :controller

  alias AppWeb.Plugs.ServerSentEvents

  plug ServerSentEvents,
    topic: fn conn -> "feed:account:#{conn.assigns.current_account.id}" end,
    pubsub: Platform.PubSub
    when action in [:stream]

  def stream(conn, _params), do: conn
end
```
