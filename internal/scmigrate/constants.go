package scmigrate

const (
	AnnotationPrefix = "scmigrate.laverya.github.com/"

	AnnState              = AnnotationPrefix + "state"
	AnnSourcePVC          = AnnotationPrefix + "source-pvc"
	AnnSourceNamespace    = AnnotationPrefix + "source-namespace"
	AnnSourceUID          = AnnotationPrefix + "source-uid"
	AnnSourcePV           = AnnotationPrefix + "source-pv"
	AnnDestinationPVC     = AnnotationPrefix + "destination-pvc"
	AnnDestinationPV      = AnnotationPrefix + "destination-pv"
	AnnTargetStorageClass = AnnotationPrefix + "target-storage-class"
	AnnOriginalPVC        = AnnotationPrefix + "original-pvc-json"
	AnnOriginalSourceRP   = AnnotationPrefix + "original-source-reclaim-policy"
	AnnOriginalDestRP     = AnnotationPrefix + "original-destination-reclaim-policy"
	AnnQuiesce            = AnnotationPrefix + "quiesce-json"

	LabelManagedBy = "app.kubernetes.io/managed-by"
	LabelRole      = AnnotationPrefix + "role"
	LabelSourceUID = AnnotationPrefix + "source-uid"

	StatePrepared      = "prepared"
	StateInitialSynced = "initial-synced"
	StateQuiesced      = "quiesced"
	StateFinalSynced   = "final-synced"
	StateCutover       = "cutover"
	StateRestored      = "restored"
)
