namespace EventPlatform.IntegrationTests.Fixtures;

/// <summary>
/// Defines an xUnit test collection for tests that rely on <see cref="CustomWebApplicationFactory"/>.
/// Placing tests in this collection ensures they run sequentially, preventing race conditions
/// caused by concurrent modification of global environment variables in
/// <see cref="CustomWebApplicationFactory.InitializeAsync"/>.
/// </summary>
[CollectionDefinition(nameof(ApiTestsCollection))]
public sealed class ApiTestsCollection;
