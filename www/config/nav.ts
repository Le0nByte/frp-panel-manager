import {
  ServerIcon,
  MonitorSmartphoneIcon,
  Cable,
  Scroll,
  ListChecks,
  Undo2,
  LayoutDashboard,
  BookOpen,
} from "lucide-react"
import { TbBuildingTunnel } from "react-icons/tb"
import { ROUTES } from "@/lib/routes"

export const teams = [
  {
    name: "Frp-Panel",
    logo: TbBuildingTunnel,
    plan: "Community Edition",
    url: "/",
  },
]

// Spec §9.1: 总览 / 穿透规则 / 端口中心 / 节点 / 实时日志 / 审计与回滚 / 系统设置.
// Worker, WireGuard and remote terminal/console entries are hidden.
export const getNavItems = (t: any) => [
  {
    title: t('nav.overview'),
    url: ROUTES.home,
    icon: LayoutDashboard,
    isActive: true,
  },
  {
    title: t('nav.editTunnel'),
    url: ROUTES.proxies,
    icon: Cable,
  },
  {
    title: t('nav.ports'),
    url: ROUTES.ports,
    icon: ListChecks,
  },
  {
    title: t('nav.clients'),
    url: "/clients",
    icon: MonitorSmartphoneIcon,
  },
  {
    title: t('nav.servers'),
    url: "/servers",
    icon: ServerIcon,
  },
  {
    title: t('nav.realTimeLog'),
    url: "/streamlog",
    icon: Scroll,
  },
  {
      title: t('nav.audit'),
      url: ROUTES.audit,
      icon: Undo2,
    },
    {
      title: t('nav.guide'),
      url: ROUTES.guide,
      icon: BookOpen,
    },
  ]
