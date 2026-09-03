package protocol

// CtlPush is phone → bridge: how to reach this phone while it is not
// connected (APNs device token and the notification kinds it wants). Sent
// after admission and again whenever the token or the preferences change.
const CtlPush = "push"

// PushRegistration rides in CtlMessage.Push.
type PushRegistration struct {
	// Hex APNs device token from the OS; empty withdraws the registration.
	Token string `json:"token"`
	// "sandbox" for development builds, "production" for TestFlight/App Store.
	Environment string `json:"environment"`
	// Notification kinds the phone wants (push.Kind values). Empty = none.
	Kinds []string `json:"kinds"`
}

// Push environments.
const (
	PushSandbox    = "sandbox"
	PushProduction = "production"
)
