import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../../app/App";

describe("Phase 6 workflow islands", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    window.localStorage.clear();
    window.sessionStorage.clear();
    window.location.hash = "";
    vi.restoreAllMocks();
  });

  it("renders the pipeline board and hides legacy pipeline only after data loads", async () => {
    const legacySurface = document.createElement("div");
    legacySurface.className = "surface";
    const legacyBoard = document.createElement("div");
    legacyBoard.id = "crmBoard";
    const view = activeView("view-crm", legacySurface, legacyBoard);
    document.body.appendChild(view);
    vi.stubGlobal("fetch", vi.fn(fetchStub));

    render(<App mode="pipeline" />);

    expect(legacySurface).not.toHaveAttribute("hidden");
    await waitFor(() => expect(screen.getByTestId("react-pipeline-page")).toBeInTheDocument());
    expect(screen.getByText("Jane Prospect")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /jane prospect/i })).toHaveAttribute("data-action", "open-crm-lead");
    expect(document.querySelectorAll(".crm-stage-column")).toHaveLength(3);
    expect(document.querySelector(".crm-stage-list[data-stage='Qualified']")).toBeInTheDocument();
    expect(legacySurface).toHaveAttribute("hidden");
    expect(legacyBoard).toHaveAttribute("hidden");
  });

  it("uses existing pipeline filters and stage update endpoints", async () => {
    const view = activeView("view-crm", document.createElement("div"));
    document.body.appendChild(view);
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="pipeline" />);

    await waitFor(() => expect(screen.getByTestId("react-pipeline-page")).toBeInTheDocument());
    const card = screen.getByRole("button", { name: /jane prospect/i });
    const wonDropTarget = document.querySelector(".crm-stage-list[data-stage='Won']");
    expect(wonDropTarget).toBeInTheDocument();
    const dataTransfer = dragDataTransfer();
    fireEvent.dragStart(card, { dataTransfer });
    fireEvent.drop(wonDropTarget as Element, { dataTransfer });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/crm/leads/42/stage",
        expect.objectContaining({ method: "PATCH" })
      );
    });
    const stageCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/ui/api/crm/leads/42/stage");
    expect(JSON.parse(stageCall?.[1]?.body as string)).toEqual({ stage: "Won" });

    fireEvent.change(screen.getByLabelText(/filter pipeline by stage/i), {
      target: { value: "Won" }
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/crm/leads?archived=false&stage=Won",
        expect.any(Object)
      );
    });
  });

  it("creates manual contacts through the existing CRM endpoint", async () => {
    const view = activeView("view-crm", document.createElement("div"));
    document.body.appendChild(view);
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="pipeline" />);

    await waitFor(() => expect(screen.getByTestId("react-pipeline-page")).toBeInTheDocument());
    fireEvent.click(screen.getAllByRole("button", { name: /add contact/i })[0]);
    fireEvent.change(screen.getByLabelText("Name"), {
      target: { value: "New Manual Lead" }
    });
    fireEvent.change(screen.getByLabelText("Phone"), {
      target: { value: "+15550001111" }
    });
    fireEvent.click(screen.getByRole("button", { name: /create contact/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/crm/leads",
        expect.objectContaining({ method: "POST" })
      );
    });
    const createCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/ui/api/crm/leads" && call[1]?.method === "POST");
    expect(JSON.parse(createCall?.[1]?.body as string)).toMatchObject({
      client_key: "demo",
      full_name: "New Manual Lead",
      phone: "+15550001111"
    });
  });

  it("opens working contact actions from pipeline cards in the React shell", async () => {
    window.location.hash = "crm";
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="app-shell" />);

    await waitFor(() => expect(screen.getByTestId("react-pipeline-page")).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: /^message$/i }));
    const dialog = await screen.findByRole("dialog", { name: /jane prospect/i });
    expect(dialog).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText(/^message$/i), { target: { value: "Following up from pipeline." } });
    fireEvent.click(screen.getByRole("button", { name: /^send message$/i }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      "/ui/api/conversations/42/messages/manual",
      expect.objectContaining({ method: "POST" })
    ));
    expect((fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/ui/api/conversations/42/messages/manual")?.[1]?.headers).toBeInstanceOf(Headers);
  });
});

describe("Phase 7 inbox island", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    window.localStorage.clear();
    window.sessionStorage.clear();
    window.location.hash = "";
    vi.restoreAllMocks();
  });

  it("renders the inbox thread and sends manual replies through the existing endpoint", async () => {
    const legacySurface = document.createElement("div");
    legacySurface.className = "surface";
    const legacyShell = document.createElement("div");
    legacyShell.id = "conversationShell";
    const view = activeView("view-conversations", legacySurface, legacyShell);
    document.body.appendChild(view);
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="inbox" />);

    await waitFor(() => expect(screen.getAllByText("Jane Prospect").length).toBeGreaterThan(0));
    expect(screen.getByTestId("react-inbox-page")).toBeInTheDocument();
    expect(document.querySelectorAll(".react-inbox-shell .drag-handle")).toHaveLength(2);
    expect(document.querySelector(".react-inbox-shell .thread-pane")).toBeInTheDocument();
    expect(document.querySelector(".react-inbox-shell .details-pane")).toBeInTheDocument();
    expect(screen.getAllByText("Can we talk tomorrow?").length).toBeGreaterThan(0);
    expect(legacyShell).toHaveAttribute("hidden");

    fireEvent.change(screen.getByPlaceholderText(/direct outbound message/i), {
      target: { value: "Tomorrow works." }
    });
    fireEvent.click(screen.getByRole("button", { name: /send message/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/conversations/42/messages/manual",
        expect.objectContaining({ method: "POST" })
      );
    });
    const sendCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/ui/api/conversations/42/messages/manual");
    const idempotencyKey = (sendCall?.[1]?.headers as Headers).get("Idempotency-Key");
    expect(idempotencyKey).toBeTruthy();
    expect(screen.getByText(/SMS delivered/i)).toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("[object Object]");
  });

  it("sends Test Lab composer messages as the sandbox lead and renders the GPT reply", async () => {
    const view = activeView("view-conversations", document.createElement("div"), document.createElement("div"));
    document.body.appendChild(view);
    let turnCompleted = false;
    const leadMessage = "Est-ce que vous êtes disponible vendredi?";
    const agentReply = "Oui, je peux vous proposer vendredi à 14 h.";
    const sandboxThread = {
      ...sampleThread,
      lead: { ...sampleThread.lead, tags: ["sandbox", "Qualified"] }
    };
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === "/ui/api/conversations/42/sandbox/messages") {
        turnCompleted = true;
        return jsonResponse({
          status: "ok",
          lead_id: 42,
          state: "QUALIFYING",
          crm_stage: "Qualified",
          delivery_mode: "sandbox",
          twilio_bypassed: true,
          inbound_message_id: 3,
          reply: { id: 4, body: agentReply, provider_message_sid: "MOCK-OUT-4" }
        });
      }
      if (url === "/ui/api/conversations/42/thread") {
        return jsonResponse({
          ...sandboxThread,
          messages: turnCompleted
            ? [
                ...sampleThread.messages,
                {
                  id: 3,
                  direction: "INBOUND",
                  body: leadMessage,
                  provider_message_sid: "SANDBOX-IN-3",
                  attachments: [],
                  delivery: null,
                  created_at: "2026-06-10T12:10:00Z"
                },
                {
                  id: 4,
                  direction: "OUTBOUND",
                  body: agentReply,
                  provider_message_sid: "MOCK-OUT-4",
                  attachments: [],
                  delivery: null,
                  created_at: "2026-06-10T12:10:01Z"
                }
              ]
            : sampleThread.messages
        });
      }
      return fetchStub(input, init);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="inbox" />);

    const composer = await screen.findByRole("textbox", { name: /test lead message/i });
    expect(composer).toHaveAttribute("placeholder", "Type the next message as the test lead.");
    expect(screen.getByText(/messages send as the test lead.*Twilio is not used/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/attach image or video/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox", { name: /pause ai/i })).not.toBeInTheDocument();

    fireEvent.change(composer, { target: { value: leadMessage } });
    fireEvent.click(screen.getByRole("button", { name: /send as test lead/i }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      "/ui/api/conversations/42/sandbox/messages",
      expect.objectContaining({ method: "POST" })
    ));
    const sandboxCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find(
      (call) => call[0] === "/ui/api/conversations/42/sandbox/messages"
    );
    expect(JSON.parse(sandboxCall?.[1]?.body as string)).toEqual({ body: leadMessage });
    expect((sandboxCall?.[1]?.headers as Headers).get("Idempotency-Key")).toBeNull();
    expect(fetchMock).not.toHaveBeenCalledWith(
      "/ui/api/conversations/42/messages/manual",
      expect.anything()
    );
    expect(await screen.findByText(agentReply)).toBeInTheDocument();
    expect(screen.getByText(leadMessage)).toBeInTheDocument();
    expect(window.localStorage.getItem("lead-ui-sandbox-lead")).toBe("42");
  });

  it("requires explicit confirmation before rotating an ambiguous outbound attempt", async () => {
    const view = activeView("view-conversations", document.createElement("div"), document.createElement("div"));
    document.body.appendChild(view);
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input) === "/ui/api/conversations/42/messages/manual") {
        return jsonResponse({ detail: "The previous delivery result is unknown; verify the conversation before retrying." }, 409);
      }
      return fetchStub(input, init);
    });
    vi.stubGlobal("fetch", fetchMock);
    const confirmMock = vi.spyOn(window, "confirm").mockReturnValue(true);

    render(<App mode="inbox" />);

    await waitFor(() => expect(screen.getAllByText("Jane Prospect").length).toBeGreaterThan(0));
    fireEvent.change(screen.getByPlaceholderText(/direct outbound message/i), {
      target: { value: "Please confirm tomorrow." }
    });
    fireEvent.click(screen.getByRole("button", { name: /send message/i }));

    const newAttemptButton = await screen.findByRole("button", { name: /start a new outbound attempt/i });
    expect(window.sessionStorage.getItem("lead-ui-outbound-request:inbox-message-42")).not.toBeNull();
    fireEvent.click(newAttemptButton);

    expect(confirmMock).toHaveBeenCalled();
    expect(window.sessionStorage.getItem("lead-ui-outbound-request:inbox-message-42")).toBeNull();
    expect(screen.getByText(/new outbound attempt is ready/i)).toBeInTheDocument();
  });

  it("scopes shell inbox requests to the selected client and debounced workspace search", async () => {
    window.location.hash = "conversations";
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="app-shell" />);

    await waitFor(() => expect(screen.getByTestId("react-inbox-page")).toBeInTheDocument());
    fireEvent.change(screen.getByRole("searchbox", { name: /search workspace/i }), { target: { value: "Jane" } });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/conversations?client_key=demo&q=Jane",
        expect.any(Object)
      );
    });
  });

  it("supports inbox state/date filters and keyboard pane resizing", async () => {
    const view = activeView("view-conversations", document.createElement("div"), document.createElement("div"));
    document.body.appendChild(view);
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="inbox" />);

    await waitFor(() => expect(screen.getByTestId("react-inbox-page")).toBeInTheDocument());
    fireEvent.change(screen.getByLabelText(/filter conversations by state/i), { target: { value: "HANDOFF" } });
    fireEvent.change(screen.getByLabelText(/conversation date from/i), { target: { value: "2026-06-01" } });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/conversations?date_from=2026-06-01&state=HANDOFF",
        expect.any(Object)
      );
    });

    const separator = screen.getByRole("separator", { name: /resize conversation list/i });
    expect(separator).toHaveAttribute("aria-valuenow", "320");
    fireEvent.keyDown(separator, { key: "ArrowRight" });
    expect(separator).toHaveAttribute("aria-valuenow", "340");
  });

  it("sends media, controls AI, sends booking links, and protects hard delete", async () => {
    const view = activeView("view-conversations", document.createElement("div"), document.createElement("div"));
    document.body.appendChild(view);
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="inbox" />);

    await waitFor(() => expect(screen.getByTestId("react-inbox-page")).toBeInTheDocument());
    const pauseCheckbox = screen.getByRole("checkbox", { name: /pause ai after this manual reply/i });
    expect(pauseCheckbox).not.toBeChecked();
    fireEvent.click(pauseCheckbox);
    const attachment = new File(["image bytes"], "photo.png", { type: "image/png", lastModified: 10 });
    fireEvent.change(screen.getByLabelText(/attach image or video/i), { target: { files: [attachment] } });
    fireEvent.click(screen.getByRole("button", { name: /^send message$/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/conversations/42/messages/manual-media",
        expect.objectContaining({ method: "POST" })
      );
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/conversations/42/agent-control",
        expect.objectContaining({ method: "PATCH" })
      );
    });
    const mediaCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/ui/api/conversations/42/messages/manual-media");
    expect(mediaCall?.[1]?.body).toBeInstanceOf(FormData);
    expect((mediaCall?.[1]?.headers as Headers).get("Idempotency-Key")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /send booking link/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      "/ui/api/conversations/42/actions/booking-link",
      expect.objectContaining({ method: "POST" })
    ));

    const confirmMock = vi.spyOn(window, "confirm").mockReturnValue(false);
    fireEvent.click(screen.getByRole("button", { name: /^delete$/i }));
    expect(confirmMock).toHaveBeenCalledWith(expect.stringMatching(/full conversation history.*cannot be undone/i));
    expect(fetchMock).not.toHaveBeenCalledWith("/ui/api/conversations/42", expect.objectContaining({ method: "DELETE" }));
  });
});

describe("Phase 8 calendar island", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    window.localStorage.clear();
    window.sessionStorage.clear();
    window.localStorage.setItem("lead-ui-selected-client", "demo");
    window.localStorage.setItem("lead-ui-calendar-month", "2026-06");
    window.localStorage.setItem("lead-ui-calendar-day", "2026-06-10");
    vi.restoreAllMocks();
  });

  it("renders the monthly calendar grid with selected-day meetings and tasks", async () => {
    const legacySurface = document.createElement("div");
    legacySurface.className = "surface";
    const legacyCalendar = document.createElement("div");
    legacyCalendar.className = "calendar-experience";
    const view = activeView("view-calendar", legacySurface, legacyCalendar);
    document.body.appendChild(view);
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="calendar" />);

    await waitFor(() => expect(screen.getByTestId("react-calendar-page")).toBeInTheDocument());
    expect(screen.getByText("June 2026")).toBeInTheDocument();
    expect(document.querySelectorAll(".calendar-month-grid .calendar-day")).toHaveLength(42);
    expect(screen.getAllByText("Strategy call").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Follow up").length).toBeGreaterThan(0);
    expect(legacySurface).toHaveAttribute("hidden");
    expect(legacyCalendar).toHaveAttribute("hidden");

    const confirmMock = vi.spyOn(window, "confirm").mockReturnValue(false);
    fireEvent.click(screen.getByText("Actions"));
    fireEvent.click(screen.getByRole("button", { name: /^delete$/i }));
    expect(confirmMock).toHaveBeenCalledWith(expect.stringMatching(/cannot be undone/i));
    expect(fetchMock).not.toHaveBeenCalledWith("/ui/api/calendar/meetings/9", expect.any(Object));
  });

  it("creates a meeting for an existing contact through the calendar endpoint", async () => {
    const view = activeView("view-calendar", document.createElement("div"), document.createElement("div"));
    document.body.appendChild(view);
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="calendar" />);

    await waitFor(() => expect(screen.getByTestId("react-calendar-page")).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: /^add meeting$/i }));
    const form = document.querySelector(".react-meeting-form") as HTMLFormElement;
    expect(form).toBeInTheDocument();
    fireEvent.submit(form);

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      "/ui/api/clients/demo/calendar/meetings",
      expect.objectContaining({ method: "POST" })
    ));
    const createCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/ui/api/clients/demo/calendar/meetings");
    expect(JSON.parse(createCall?.[1]?.body as string)).toMatchObject({
      duration_minutes: 30,
      lead_id: 42,
      timezone: "America/Toronto",
      title: "Discovery meeting"
    });
  });
});

describe("Phase 5 tasks island", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    window.localStorage.clear();
    window.sessionStorage.clear();
    vi.restoreAllMocks();
  });

  it("renders the grouped task queue and hides the legacy tasks table", async () => {
    const legacySurface = document.createElement("div");
    legacySurface.className = "surface";
    const legacyTable = document.createElement("div");
    legacyTable.className = "surface stack";
    const view = activeView("view-tasks", legacySurface, legacyTable);
    document.body.appendChild(view);
    vi.stubGlobal("fetch", vi.fn(fetchStub));

    render(<App mode="tasks" />);

    await waitFor(() => expect(screen.getByTestId("react-tasks-page")).toBeInTheDocument());
    expect(screen.getByRole("combobox", { name: /filter tasks by status/i })).toBeInTheDocument();
    expect(screen.getByText("Overdue")).toBeInTheDocument();
    expect(screen.getByText("No due date")).toBeInTheDocument();
    expect(screen.getAllByText("Done").length).toBeGreaterThan(0);
    expect(screen.getByText("Follow up")).toBeInTheDocument();
    expect(screen.getByText("Back office cleanup")).toBeInTheDocument();
    expect(screen.getByText("Completed task")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: /message/i }).length).toBeGreaterThan(0);
    expect(screen.getAllByRole("button", { name: /open/i }).length).toBeGreaterThan(0);
    expect(legacySurface).toHaveAttribute("hidden");
    expect(legacyTable).toHaveAttribute("hidden");
  });

  it("uses existing task filter and update endpoints", async () => {
    const view = activeView("view-tasks", document.createElement("div"), document.createElement("div"));
    document.body.appendChild(view);
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="tasks" />);

    await waitFor(() => expect(screen.getByTestId("react-tasks-page")).toBeInTheDocument());
    fireEvent.change(screen.getByRole("combobox", { name: /filter tasks by status/i }), {
      target: { value: "done" }
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith("/ui/api/crm/tasks?status=done", expect.any(Object));
    });

    fireEvent.change(screen.getByRole("searchbox", { name: /search tasks/i }), {
      target: { value: "Jane" }
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith("/ui/api/crm/tasks?q=Jane&status=done", expect.any(Object));
    });

    fireEvent.click(screen.getAllByRole("button", { name: /^done$/i })[0]);

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith("/ui/api/crm/tasks/10", expect.objectContaining({ method: "PATCH" }));
    });
  });
});

describe("Records island", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    window.localStorage.clear();
    window.sessionStorage.clear();
    vi.restoreAllMocks();
  });

  it("renders lead details from the CRM endpoint and hides the legacy records view", async () => {
    const legacySurface = document.createElement("div");
    legacySurface.className = "surface";
    const legacyShell = document.createElement("div");
    legacyShell.className = "two-column-shell";
    const view = activeView("view-leads", legacySurface, legacyShell);
    document.body.appendChild(view);
    vi.stubGlobal("fetch", vi.fn(fetchStub));

    render(<App mode="records" />);

    await waitFor(() => expect(screen.getByTestId("react-records-page")).toBeInTheDocument());
    expect(screen.getAllByText("Jane Prospect").length).toBeGreaterThan(0);
    expect(screen.getByText("Form answers")).toBeInTheDocument();
    expect(screen.getByText("CRM insight")).toBeInTheDocument();
    expect(screen.getByText("Recent messages")).toBeInTheDocument();
    expect(screen.getByText("+1 (555) 123-4567")).toBeInTheDocument();
    expect(legacySurface).toHaveAttribute("hidden");
    expect(legacyShell).toHaveAttribute("hidden");
  });

  it("preserves records actions on the existing CRM endpoints", async () => {
    const view = activeView("view-leads", document.createElement("div"), document.createElement("div"));
    document.body.appendChild(view);
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="records" />);

    await waitFor(() => expect(screen.getByTestId("react-records-page")).toBeInTheDocument());

    fireEvent.change(screen.getByLabelText("Stage"), { target: { value: "Won" } });
    fireEvent.click(screen.getByRole("button", { name: /update stage/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/crm/leads/42/stage", expect.objectContaining({ method: "PATCH" })));

    const tagInput = screen.getByPlaceholderText("add tag");
    fireEvent.change(tagInput, { target: { value: "hot" } });
    fireEvent.submit(tagInput.closest("form") as HTMLFormElement);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/crm/leads/42/tags", expect.objectContaining({ method: "POST" })));

    const noteInput = screen.getByPlaceholderText(/internal note/i);
    fireEvent.change(noteInput, { target: { value: "Call before Friday." } });
    fireEvent.submit(noteInput.closest("form") as HTMLFormElement);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/crm/leads/42/notes", expect.objectContaining({ method: "POST" })));

    fireEvent.click(screen.getAllByText("Add task")[0]);
    fireEvent.change(screen.getByPlaceholderText("Task title"), { target: { value: "Send recap" } });
    fireEvent.change(screen.getByPlaceholderText("Optional details"), { target: { value: "Include timeline." } });
    fireEvent.click(screen.getByRole("button", { name: /^add task$/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/crm/leads/42/tasks", expect.objectContaining({ method: "POST" })));

    fireEvent.click(screen.getAllByRole("button", { name: /done/i })[0]);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/crm/tasks/10", expect.objectContaining({ method: "PATCH" })));

    fireEvent.click(screen.getByRole("button", { name: /^archive$/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/conversations/42/archive", expect.objectContaining({ method: "PATCH" })));
  });
});

function activeView(id: string, ...children: HTMLElement[]) {
  const view = document.createElement("section");
  view.id = id;
  view.className = "view active";
  children.forEach((child) => view.appendChild(child));
  return view;
}

async function fetchStub(input: RequestInfo | URL, init?: RequestInit) {
  const url = String(input);
  if (url === "/ui/api/session") return jsonResponse(sampleSession);
  if (url === "/ui/api/clients") return jsonResponse(sampleClients);
  if (url === "/ui/api/clients/demo/calendar") return jsonResponse(sampleCalendar);
  if (url === "/ui/api/clients/demo/calendar/meetings") return jsonResponse({ status: "ok", meeting: sampleCalendar.items[0] });
  if (url === "/ui/api/crm/tasks" || url.startsWith("/ui/api/crm/tasks?")) return jsonResponse(sampleTasks);
  if (url === "/ui/api/crm/leads" && init?.method === "POST") return jsonResponse({ status: "ok", lead: { id: 99, lead_id: 99, display_name: "New Manual Lead", client_key: "demo", crm_stage: "New Lead", conversation_state: "NEW" } });
  if (url === "/ui/api/crm/leads/42") return jsonResponse(sampleLeadDetail);
  if (url.startsWith("/ui/api/crm/leads?") || url === "/ui/api/crm/leads") return jsonResponse(sampleLeads);
  if (url === "/ui/api/crm/leads/42/stage") return jsonResponse({ status: "ok", lead_id: 42, crm_stage: "Won", changed: true });
  if (url === "/ui/api/crm/leads/42/tags") return jsonResponse({ status: "ok", tags: ["Qualified", "hot"] });
  if (url === "/ui/api/crm/leads/42/notes") return jsonResponse({ status: "ok", note: { id: 11 } });
  if (url === "/ui/api/crm/leads/42/tasks") return jsonResponse({ status: "ok", task: { id: 12 } });
  if (url === "/ui/api/crm/tasks/10") return jsonResponse({ status: "ok", task: { id: 10, status: "done" } });
  if (url === "/ui/api/conversations/42/archive") return jsonResponse({ status: "ok", lead_id: 42, archived: true, changed: true, tags: ["archived"] });
  if (url === "/ui/api/conversations/42/thread") return jsonResponse(sampleThread);
  if (url === "/ui/api/conversations/42/messages/manual") {
    return jsonResponse({ status: "ok", lead_id: 42, provider_sid: "SM1", state: "GREETED" });
  }
  if (url === "/ui/api/conversations/42/messages/manual-media") return jsonResponse({ status: "ok", lead_id: 42, provider_sid: "MM1", state: "GREETED" });
  if (url === "/ui/api/conversations/42/actions/booking-link") return jsonResponse({ status: "ok", provider_sid: "SM2", body: "Book now", state: "BOOKING_SENT" });
  if (url === "/ui/api/conversations/42/agent-control") return jsonResponse({ status: "ok", lead_id: 42, state: "QUALIFYING", agent_control: { paused: true, mode: "paused" } });
  if (url === "/ui/api/conversations/42" && init?.method === "DELETE") return jsonResponse({ status: "ok", deleted_lead_id: 42 });
  if (url === "/ui/api/conversations" || url.startsWith("/ui/api/conversations?")) return jsonResponse(sampleConversations);
  return jsonResponse({ detail: `Unhandled ${url}` }, 404);
}

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}

function dragDataTransfer() {
  const values = new Map<string, string>();
  return {
    getData: vi.fn((type: string) => values.get(type) || ""),
    setData: vi.fn((type: string, value: string) => values.set(type, value))
  };
}

const sampleSession = {
  status: "ok",
  role: "admin",
  app_name: "Lead Ops Console",
  env: "test",
  generated_at: "2026-06-10T12:00:00Z",
  can_seed_demo: true,
  demo_data_present: true,
  client_key: null,
  client_name: null,
  portal_display_name: null
};

const sampleClients = [
  {
    client_key: "demo",
    business_name: "Demo Client",
    is_active: true,
    created_at: "2026-06-01T00:00:00Z"
  }
];

const sampleLead = {
  lead_id: 42,
  lead_name: "Jane Prospect",
  phone: "+15551234567",
  email: "jane@example.com",
  source: "meta",
  client_key: "demo",
  client_name: "Demo Client",
  crm_stage: "Qualified",
  conversation_state: "QUALIFYING",
  agent_control: {},
  last_message_snippet: "Can we talk tomorrow?",
  last_message_direction: "INBOUND",
  last_message_delivery: null,
  lead_summary: "Timeline: soon",
  last_activity_at: "2026-06-10T12:00:00Z",
  created_at: "2026-06-10T11:00:00Z",
  tags: ["Qualified"],
  booked: false,
  archived: false
};

const sampleLeads = {
  items: [sampleLead],
  counts: { Qualified: 1 },
  total: 1,
  stages: ["New Lead", "Qualified", "Won"]
};

const sampleConversations = {
  items: [
    {
      lead_id: 42,
      lead_name: "Jane Prospect",
      phone: "+15551234567",
      source: "meta",
      client_key: "demo",
      client_name: "Demo Client",
      state: "QUALIFYING",
      crm_stage: "Qualified",
      tags: ["Qualified"],
      last_message_snippet: "Can we talk tomorrow?",
      last_activity_at: "2026-06-10T12:00:00Z"
    }
  ],
  counts: { QUALIFYING: 1 },
  total: 1
};

const sampleCalendar = {
  client_key: "demo",
  booking_mode: "internal",
  timezone: "America/Toronto",
  total: 1,
  items: [
    {
      id: 9,
      lead_id: 42,
      lead_name: "Jane Prospect",
      lead_phone: "+15551234567",
      title: "Strategy call",
      status: "scheduled",
      start_at: "2026-06-10T15:00:00Z",
      end_at: "2026-06-10T15:30:00Z",
      timezone: "America/Toronto",
      notes: "Bring source details."
    }
  ]
};

const sampleTasks = {
  items: [
    {
      id: 10,
      lead_id: 42,
      title: "Follow up",
      description: "Send recap",
      due_date: "2026-06-10",
      status: "open",
      lead_name: "Jane Prospect",
      lead_phone: "+15551234567",
      client_key: "demo",
      client_name: "Demo Client"
    },
    {
      id: 11,
      lead_id: 42,
      title: "Back office cleanup",
      description: "",
      due_date: null,
      status: "open",
      lead_name: "Jane Prospect",
      lead_phone: "+15551234567",
      client_key: "demo",
      client_name: "Demo Client"
    },
    {
      id: 12,
      lead_id: 42,
      title: "Completed task",
      description: "Already handled",
      due_date: "2026-06-09",
      status: "done",
      lead_name: "Jane Prospect",
      lead_phone: "+15551234567",
      client_key: "demo",
      client_name: "Demo Client"
    }
  ],
  counts: { open: 2, done: 1 },
  total: 3
};

const sampleThread = {
  lead: {
    id: 42,
    display_name: "Jane Prospect",
    full_name: "Jane Prospect",
    phone: "+15551234567",
    email: "jane@example.com",
    source: "meta",
    form_answers: { timeline: "Tomorrow" },
    summary_lines: ["Timeline: Tomorrow"],
    agent_control: {},
    current_state: "QUALIFYING",
    crm_stage: "Qualified",
    opted_out: false,
    created_at: "2026-06-10T11:00:00Z",
    updated_at: "2026-06-10T12:00:00Z",
    last_activity_at: "2026-06-10T12:00:00Z",
    tags: ["Qualified"]
  },
  client: {
    client_key: "demo",
    business_name: "Demo Client",
    booking_url: "https://example.test/book",
    fallback_handoff_number: "",
    tone: "friendly"
  },
  messages: [
    {
      id: 1,
      direction: "INBOUND",
      body: "Can we talk tomorrow?",
      provider_message_sid: "SM-IN",
      attachments: [],
      delivery: null,
      created_at: "2026-06-10T12:00:00Z"
    },
    {
      id: 2,
      direction: "OUTBOUND",
      body: "Tomorrow works.",
      provider_message_sid: "SM-OUT",
      attachments: [],
      delivery: { status: "delivered", label: "SMS delivered", severity: "success" },
      created_at: "2026-06-10T12:05:00Z"
    }
  ],
  notes: [],
  tasks: [],
  audit_events: [],
  timeline: []
};

const sampleLeadDetail = {
  lead: {
    ...sampleThread.lead,
    owner: "Mike",
    summary_lines: [{ question: "Timeline", answer: "Tomorrow" }],
    lead_score: 86,
    estimated_value: 12000,
    campaign_name: "June acquisition",
    intent_level: "High",
    recommended_follow_up: "Confirm a discovery call"
  },
  client: sampleThread.client,
  messages: sampleThread.messages,
  notes: [
    {
      id: 4,
      note: "Prep call context.",
      created_at: "2026-06-10T12:05:00Z"
    }
  ],
  tasks: sampleTasks.items,
  tags: ["Qualified"],
  timeline: [
    {
      id: 8,
      event_type: "stage_changed",
      created_at: "2026-06-10T12:03:00Z",
      detail: "Qualified"
    }
  ],
  audit_events: [],
  stages: sampleLeads.stages
};
