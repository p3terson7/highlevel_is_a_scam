import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { AUTH_EXPIRED_EVENT, ApiError, clearAuthTokens, fetchSession, loginAdmin as createAdminSession, loginClient, logoutSession, storeAdminAuth, storePortalAuth } from "../../api/client";
import type { SessionPayload } from "../../api/types";

type AuthState =
  | { status: "loading"; session: null; error: null }
  | { status: "ready"; session: SessionPayload; error: string | null }
  | { status: "unauthenticated"; session: null; error: string }
  | { status: "error"; session: null; error: string };

type AuthActions = {
  refresh: () => Promise<void>;
  loginAdmin: (token: string) => Promise<void>;
  loginClientPortal: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
};

type AuthContextValue = AuthState & AuthActions;

const noopAsync = async () => {};

const AuthContext = createContext<AuthContextValue>({
  status: "loading",
  session: null,
  error: null,
  refresh: noopAsync,
  loginAdmin: noopAsync,
  loginClientPortal: noopAsync,
  logout: noopAsync
});

type AuthProviderProps = {
  children: ReactNode;
};

export function AuthProvider({ children }: AuthProviderProps) {
  const [state, setState] = useState<AuthState>({ status: "loading", session: null, error: null });

  const refresh = useCallback(async () => {
    setState({ status: "loading", session: null, error: null });
    try {
      const session = await fetchSession();
      setState({ status: "ready", session, error: null });
    } catch (error: unknown) {
      if (error instanceof ApiError && error.status === 401) {
        setState({ status: "unauthenticated", session: null, error: error.message });
        throw error;
      }
      const message = error instanceof Error ? error.message : "Session unavailable";
      setState({ status: "error", session: null, error: message });
      throw error;
    }
  }, []);

  const loginAdmin = useCallback(
    async (token: string) => {
      const trimmedToken = token.trim();
      if (!trimmedToken) {
        setState({ status: "unauthenticated", session: null, error: "Admin token is required" });
        throw new Error("Admin token is required");
      }
      const result = await createAdminSession(trimmedToken);
      storeAdminAuth();
      setState({ status: "ready", session: result.session, error: null });
    },
    [refresh]
  );

  const loginClientPortal = useCallback(async (email: string, password: string) => {
    try {
      const result = await loginClient(email, password);
      storePortalAuth();
      setState({ status: "ready", session: result.session, error: null });
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : "Client login failed";
      setState({ status: "unauthenticated", session: null, error: message });
      throw error;
    }
  }, []);

  const logout = useCallback(async () => {
    setState((current) => current.status === "ready" ? { ...current, error: null } : current);
    try {
      await logoutSession();
    } catch (error: unknown) {
      if (!(error instanceof ApiError && error.status === 401)) {
        const detail = error instanceof Error ? error.message : "The server did not confirm the request.";
        const message = `Sign out failed. Your session may still be active; please retry. ${detail}`;
        setState((current) => {
          if (current.status === "ready" || current.status === "unauthenticated" || current.status === "error") {
            return { ...current, error: message };
          }
          return current;
        });
        return;
      }
    }
    clearAuthTokens();
    setState({ status: "unauthenticated", session: null, error: "Signed out" });
  }, []);

  useEffect(() => {
    let cancelled = false;

    setState({ status: "loading", session: null, error: null });
    fetchSession()
      .then((session) => {
        if (!cancelled) setState({ status: "ready", session, error: null });
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        if (error instanceof ApiError && error.status === 401) {
          setState({ status: "unauthenticated", session: null, error: error.message });
          return;
        }
        setState({ status: "error", session: null, error: error instanceof Error ? error.message : "Session unavailable" });
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const value = useMemo(
    () => ({
      ...state,
      refresh,
      loginAdmin,
      loginClientPortal,
      logout
    }),
    [loginAdmin, loginClientPortal, logout, refresh, state]
  );

  useEffect(() => {
    const expireSession = () => {
      clearAuthTokens();
      setState({ status: "unauthenticated", session: null, error: "Your session expired. Sign in again." });
    };
    window.addEventListener(AUTH_EXPIRED_EVENT, expireSession);
    return () => window.removeEventListener(AUTH_EXPIRED_EVENT, expireSession);
  }, []);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  return useContext(AuthContext);
}
