package curator

type CuratorTransaction interface {
	// Start a create builder in the transaction
	Create() TransactionCreateBuilder

	// Start a delete builder in the transaction
	Delete() TransactionDeleteBuilder

	// Start a set data builder in the transaction
	SetData() TransactionSetDataBuilder

	// Start a check builder in the transaction
	Check() TransactionCheckBuilder
}
