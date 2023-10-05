## Getting Salesforce Credentials

Here's a detailed guide on how to obtain the client_id and client_secret for OAuth in Salesforce, specifically for the non-interactive server-to-server flow (using the OAuth 2.0 JWT Bearer Token Flow):

Step 1: Create a Connected App in Salesforce
Login to Salesforce:

Log in to your Salesforce account.
Navigate to App Manager:

For Lightning Experience: Click on the gear icon (⚙️) at the top right > select 'Setup'. In the left sidebar, under 'Platform Tools', click on 'Apps' > 'App Manager'.
For Salesforce Classic: Click on 'Setup' at the top right. Under 'Build', expand 'Create' and select 'Apps'.
Create a New Connected App:

Click on the 'New Connected App' button.
Fill in the Details:

Enter the required details such as Connected App Name, API Name, and Contact Email.
In the API (Enable OAuth Settings) section, check the box for 'Enable OAuth Settings'.
Enter a Callback URL. For JWT Bearer Token Flow, this can be a dummy URL, e.g., http://localhost/callback.
Under 'Selected OAuth Scopes', add the necessary scopes. For server-to-server interactions, 'Full access (full)' is often used, but you should limit the scope as much as possible for security reasons.
Save the App:

Click 'Save'. After saving, you'll be presented with the Consumer Key (which is the client_id) and Consumer Secret (which is the client_secret).
Step 2: Generate a Certificate
For the JWT Bearer Token Flow, you'll also need a digital certificate.

You can use OpenSSL to create a self-signed certificate:

bash
Copy code
openssl req -newkey rsa:2048 -nodes -keyout server.key -x509 -days 365 -out server.crt
This will generate both a private key (server.key) and a certificate (server.crt).

Step 3: Upload the Certificate to the Connected App
Go back to your Connected App in Salesforce (Setup > Apps > App Manager > Your Connected App).
Click on 'Edit' and scroll down to the 'Use digital signatures' section.
Upload the certificate (server.crt) that you generated.
Save the changes.
Step 4: Allow Users to Use the Connected App
Ensure that the user profile or permission set associated with the server-to-server interactions has the necessary permissions to use the connected app. This is typically done under 'Manage Profiles' or 'Manage Permission Sets' within the Connected App settings.
