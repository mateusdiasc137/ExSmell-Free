```elixir
defmodule Platform.WebhookSignaturePlug do
  @moduledoc """
  Verifies inbound webhook signatures before the request body reaches any
  controller. The raw request body must be read and stored before this plug
  runs. Signature verification is delegated to `Integration.WebhookVerifier`
  so provider-specific logic remains encapsulated. Requests with invalid
  signatures are rejected with 401 before any business logic executes.
  """

  @behaviour Plug

  import Plug.Conn

  @provider_header "x-webhook-provider"

  @impl Plug
  @spec init(keyword()) :: keyword()
  def init(opts), do: opts

  @impl Plug
  @spec call(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def call(%Plug.Conn{} = conn, _opts) do
    with {:ok, provider} <- resolve_provider(conn),
         {:ok, raw_body} <- get_raw_body(conn),
         :ok <- Integration.WebhookVerifier.verify(provider, header_map(conn), raw_body) do
      assign(conn, :webhook_provider, provider)
    else
      {:error, :unknown_provider} -> reject(conn, "unknown_provider")
      {:error, :missing_header} -> reject(conn, "missing_signature")
      {:error, :invalid_signature} -> reject(conn, "invalid_signature")
      {:error, :expired} -> reject(conn, "signature_expired")
      {:error, _} -> reject(conn, "verification_failed")
    end
  end

  defp resolve_provider(conn) do
    case get_req_header(conn, @provider_header) do
      [provider | _] when is_binary(provider) ->
        case provider do
          p when p in ["stripe", "github", "shopify", "twilio"] ->
            {:ok, String.to_existing_atom(p)}
          _ ->
            {:error, :unknown_provider}
        end

      [] ->
        {:error, :unknown_provider}
    end
  end

  defp get_raw_body(conn) do
    case conn.assigns[:raw_body] do
      body when is_binary(body) -> {:ok, body}
      nil ->
        case Plug.Conn.read_body(conn) do
          {:ok, body, _conn} -> {:ok, body}
          _ -> {:error, :body_unavailable}
        end
    end
  end

  defp header_map(conn) do
    Map.new(conn.req_headers, fn {k, v} -> {k, v} end)
  end

  defp reject(conn, reason) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(401, Jason.encode!(%{error: "webhook_verification_failed", reason: reason}))
    |> halt()
  end
end
```
