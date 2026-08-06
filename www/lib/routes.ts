export const ROUTES = {
	home: '/',
	proxies: '/proxies',
	ports: '/ports',
	audit: '/audit',
	workers: '/workers',
	workerEdit: '/worker-edit',
	wg: {
		networks: '/wg/networks',
		networkDetail: '/wg/network-detail',
		wireguards: '/wg/wireguards',
		wireguardDetail: '/wg/wireguard-detail',
		endpoints: '/wg/endpoints',
		endpointDetail: '/wg/endpoint-detail',
		links: '/wg/links',
	},
} as const
